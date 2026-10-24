package pathsearch

import (
	"context"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"google.golang.org/grpc"
)

type fakePathSearchServiceClient struct {
	suggestReq  *pb.SuggestRequest
	suggestResp *pb.SuggestResponse
	deadline    time.Time
	hasDeadline bool
}

func (*fakePathSearchServiceClient) PathCandidates(
	context.Context,
	*pb.PathCandidatesRequest,
	...grpc.CallOption,
) (*pb.PathCandidatesResponse, error) {
	panic("unexpected PathCandidates call")
}

func (f *fakePathSearchServiceClient) Suggest(
	ctx context.Context,
	req *pb.SuggestRequest,
	_ ...grpc.CallOption,
) (*pb.SuggestResponse, error) {
	f.suggestReq = req
	f.deadline, f.hasDeadline = ctx.Deadline()

	return f.suggestResp, nil
}

func (*fakePathSearchServiceClient) HealthCheck(
	context.Context,
	*pb.HealthCheckRequest,
	...grpc.CallOption,
) (*pb.PathSearchHealth, error) {
	panic("unexpected HealthCheck call")
}

func TestParseTarget(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"bitmagnet-pathsearch.bitmagnet.svc:50053", "bitmagnet-pathsearch.bitmagnet.svc:50053", false},
		{"127.0.0.1:50053", "127.0.0.1:50053", false},
		{"unix:/run/bitmagnet/pathsearch.sock", "unix:/run/bitmagnet/pathsearch.sock", false},
		{"unix:///run/bitmagnet/pathsearch.sock", "unix:///run/bitmagnet/pathsearch.sock", false},
		{"/run/bitmagnet/pathsearch.sock", "unix:///run/bitmagnet/pathsearch.sock", false},
		{"  127.0.0.1:50053  ", "127.0.0.1:50053", false},
		{"", "", true},
		{"   ", "", true},
	} {
		got, err := parseTarget(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseTarget(%q): expected error", tc.in)
			}

			continue
		}

		if err != nil {
			t.Errorf("parseTarget(%q): unexpected error %v", tc.in, err)

			continue
		}

		if got != tc.want {
			t.Errorf("parseTarget(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestClientSuggestForwardsRequestResponseAndAppliesTimeout(t *testing.T) {
	t.Parallel()

	const timeout = 500 * time.Millisecond

	req := &pb.SuggestRequest{Prefix: "movies/i", Limit: 7}
	resp := &pb.SuggestResponse{Suggestions: []*pb.Suggestion{{Value: "inception"}}}
	rpc := &fakePathSearchServiceClient{suggestResp: resp}
	client := &Client{svc: rpc, timeout: timeout}

	got, err := client.Suggest(context.Background(), req)
	if err != nil {
		t.Fatalf("Suggest: %v", err)
	}

	if rpc.suggestReq != req {
		t.Fatalf("Suggest request = %p, want original request %p", rpc.suggestReq, req)
	}

	if got != resp {
		t.Fatalf("Suggest response = %p, want fake response %p", got, resp)
	}

	if !rpc.hasDeadline {
		t.Fatal("Suggest context must carry the configured timeout deadline")
	}

	if remaining := time.Until(rpc.deadline); remaining <= 0 || remaining > timeout {
		t.Fatalf("Suggest deadline remaining = %v, want within (0, %v]", remaining, timeout)
	}
}
