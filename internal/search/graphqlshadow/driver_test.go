package graphqlshadow

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type spyExecutor struct {
	calls    atomic.Int64
	result   GraphQLResult
	duration time.Duration
	err      error
	check    func(context.Context)
}

const testInferID = "aa:movie:imdb:tt1"

func (s *spyExecutor) Execute(ctx context.Context, _ Request) (ExecutionResult, error) {
	s.calls.Add(1)

	if s.check != nil {
		s.check(ctx)
	}

	return ExecutionResult{Result: s.result, HandlerDuration: s.duration}, s.err
}

const comparableQuery = `query Search($limit: Int) {
  torrentContent {
    search(input: {limit: $limit, totalCount: true}) {
      totalCount
      totalCountIsEstimate
      items { id }
	  aggregations { __typename }
    }
  }
}`

// TestShadowCapturedUnsafeOperationsMakeZeroRustCalls is the load-bearing
// defense-in-depth test: no non-query or unclassifiable operation can reach the
// dark service even if a caller bypasses Hook and invokes Driver directly.
func TestShadowCapturedUnsafeOperationsMakeZeroRustCalls(t *testing.T) {
	t.Parallel()

	tests := []Request{
		{Query: `mutation { torrent { delete(infoHashes: ["aa"]) } }`},
		{Query: `subscription S { version }`},
		{Query: `this is not valid graphql`},
		{Query: `query Q { version } mutation M { torrent { reprocess(infoHashes: []) } }`},
		{Query: `query Q { version } mutation M { torrent { reprocess(infoHashes: []) } }`, OperationName: "M"},
		{Query: `{ version }`},
		{Query: `{ torrentContent { search { totalCount } } }`},
	}

	for _, req := range tests {
		rust := &spyExecutor{}
		driver := NewDriver(rust, nil)

		outcome, comparison, err := driver.ShadowCaptured(
			context.Background(), req, GraphQLResult{}, time.Millisecond,
		)
		if err != nil {
			t.Errorf("ShadowCaptured(%q) error: %v", req.Query, err)
		}

		if outcome != OutcomeDropped || comparison != nil {
			t.Errorf("ShadowCaptured(%q) = (%q, %v), want dropped/nil", req.Query, outcome, comparison)
		}

		if got := rust.calls.Load(); got != 0 {
			t.Errorf("SAFETY VIOLATION: ShadowCaptured(%q) called Rust %d times, want 0", req.Query, got)
		}
	}
}

func TestShadowCapturedComparesRustWithCapturedReferenceAndLatency(t *testing.T) {
	t.Parallel()

	rust := &spyExecutor{result: GraphQLResult{IDs: []string{"a"}, TotalCount: 1}}
	driver := NewDriver(rust, nil)

	rust.duration = 7 * time.Millisecond

	outcome, comparison, err := driver.ShadowCaptured(
		context.Background(),
		Request{Query: comparableQuery, OperationName: "Search", Variables: json.RawMessage(`{"limit":20}`)},
		GraphQLResult{IDs: []string{"a"}, TotalCount: 1},
		23*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("ShadowCaptured error: %v", err)
	}

	if outcome != OutcomeCompared || comparison == nil {
		t.Fatalf("outcome/comparison = %q/%v, want compared/non-nil", outcome, comparison)
	}

	if rust.calls.Load() != 1 {
		t.Fatalf("Rust calls = %d, want 1", rust.calls.Load())
	}

	if comparison.TantivyLatency != 7*time.Millisecond {
		t.Errorf("Rust latency = %s, want 7ms", comparison.TantivyLatency)
	}

	if comparison.PGLatency != 23*time.Millisecond {
		t.Errorf("captured Go latency = %s, want 23ms", comparison.PGLatency)
	}
}

func TestShadowCapturedRustError(t *testing.T) {
	t.Parallel()

	rust := &spyExecutor{err: errors.New("boom")}
	driver := NewDriver(rust, nil)

	outcome, comparison, err := driver.ShadowCaptured(
		context.Background(),
		Request{Query: comparableQuery, OperationName: "Search", Variables: json.RawMessage(`{"limit":20}`)},
		GraphQLResult{},
		time.Millisecond,
	)
	if err == nil || outcome != OutcomeRustError || comparison != nil {
		t.Fatalf("got outcome=%q comparison=%v err=%v, want rust_error/nil/error", outcome, comparison, err)
	}
}
