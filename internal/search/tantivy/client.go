// Package tantivy is the Go client for the Rust Tantivy search sidecar
// (bitmagnet-rs/crates/bitmagnet-search). It wraps the generated gRPC
// SearchServiceClient (see ./pb) and builds the proto documents the sidecar
// indexes, keeping the live Go path byte-identical to the Rust backfill so a
// shadow / dual-write upsert collapses onto the same document the backfill
// produces. See BuildDocument and DocID in document.go.
package tantivy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config configures the sidecar Client. The wiring layer supplies it.
type Config struct {
	// Address of the sidecar. Either a Unix socket ("unix:/run/bitmagnet.sock",
	// "unix:///run/bitmagnet.sock", or a bare absolute path "/run/bitmagnet.sock")
	// or a TCP "host:port" (e.g. "127.0.0.1:3334").
	Address string
	// Timeout bounds each unary RPC (IndexDocument, DeleteDocument, Search,
	// GetFacets, HealthCheck). <= 0 leaves the timeout to the caller's context.
	Timeout time.Duration
	// BatchTimeout bounds a whole BatchIndex stream. <= 0 leaves it to the
	// caller's context (a long backfill may legitimately run unbounded).
	BatchTimeout time.Duration
}

// Client is a thin, safe wrapper over the generated SearchServiceClient. It is
// safe for concurrent use: the underlying *grpc.ClientConn multiplexes calls.
type Client struct {
	conn         *grpc.ClientConn
	svc          pb.SearchServiceClient
	timeout      time.Duration
	batchTimeout time.Duration
}

// NewClient dials (lazily — grpc.NewClient connects on first RPC) the sidecar at
// cfg.Address. Use HealthCheck to verify the connection is actually serving.
func NewClient(cfg Config) (*Client, error) {
	target, err := parseTarget(cfg.Address)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("tantivy: dial %q: %w", target, err)
	}

	return &Client{
		conn:         conn,
		svc:          pb.NewSearchServiceClient(conn),
		timeout:      cfg.Timeout,
		batchTimeout: cfg.BatchTimeout,
	}, nil
}

// parseTarget normalises a config address into a gRPC target string, choosing
// the Unix-socket or TCP transport. grpc-go's built-in "unix" and default "dns"
// resolvers do the rest, so both transports dial with the same options.
func parseTarget(address string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", errors.New("tantivy: empty address")
	}

	switch {
	// Scheme-qualified Unix socket: "unix:/path", "unix:///path",
	// "unix://host/path" — grpc-go's unix resolver understands them all.
	case strings.HasPrefix(address, "unix:"):
		return address, nil
	// Bare absolute path -> Unix socket (convenience for plain-path config).
	case strings.HasPrefix(address, "/"):
		return "unix://" + address, nil
	// Otherwise host:port over TCP, resolved by the default dns scheme.
	default:
		return address, nil
	}
}

// Close tears down the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// IndexDocument upserts a single document by its composite DocID.
func (c *Client) IndexDocument(ctx context.Context, doc *pb.TorrentDocument) (*pb.IndexDocumentResponse, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()

	return c.svc.IndexDocument(ctx, &pb.IndexDocumentRequest{Document: doc})
}

// DeleteDocument removes every document for the given 20-byte info hash (the
// sidecar deletes by info_hash, clearing all of a torrent's classifications).
func (c *Client) DeleteDocument(ctx context.Context, infoHash []byte) (*pb.DeleteDocumentResponse, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()

	return c.svc.DeleteDocument(ctx, &pb.DeleteDocumentRequest{InfoHash: infoHash})
}

// Search runs a full-text + filtered query and returns ranked hits.
func (c *Client) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()

	return c.svc.Search(ctx, req)
}

// GetFacets returns faceted aggregation counts for the given query / filters.
func (c *Client) GetFacets(ctx context.Context, req *pb.GetFacetsRequest) (*pb.GetFacetsResponse, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()

	return c.svc.GetFacets(ctx, req)
}

// HealthCheck probes the sidecar's liveness / readiness (and doc count).
func (c *Client) HealthCheck(ctx context.Context) (*pb.HealthCheckResponse, error) {
	ctx, cancel := c.callCtx(ctx)
	defer cancel()

	return c.svc.HealthCheck(ctx, &pb.HealthCheckRequest{})
}

// BatchIndex streams documents read from docs to the sidecar for bulk indexing,
// returning the indexed / error counts once docs is closed. The caller closes
// docs to end the stream; a cancelled context aborts it early. Streaming (rather
// than a slice) keeps a large resync from materialising every document at once.
func (c *Client) BatchIndex(ctx context.Context, docs <-chan *pb.TorrentDocument) (*pb.BatchIndexResponse, error) {
	if c.batchTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.batchTimeout)
		defer cancel()
	}

	stream, err := c.svc.BatchIndex(ctx)
	if err != nil {
		return nil, fmt.Errorf("tantivy: open BatchIndex stream: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case doc, ok := <-docs:
			if !ok {
				resp, err := stream.CloseAndRecv()
				if err != nil {
					return nil, fmt.Errorf("tantivy: BatchIndex close: %w", err)
				}

				return resp, nil
			}

			if err := stream.Send(&pb.IndexDocumentRequest{Document: doc}); err != nil {
				// A broken stream surfaces as io.EOF on Send; the real status is
				// on CloseAndRecv.
				if errors.Is(err, io.EOF) {
					if _, recvErr := stream.CloseAndRecv(); recvErr != nil {
						return nil, fmt.Errorf("tantivy: BatchIndex send: %w", recvErr)
					}
				}

				return nil, fmt.Errorf("tantivy: BatchIndex send: %w", err)
			}
		}
	}
}

// callCtx applies the per-call timeout when configured, otherwise returns ctx
// unchanged with a no-op cancel.
func (c *Client) callCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, c.timeout)
}
