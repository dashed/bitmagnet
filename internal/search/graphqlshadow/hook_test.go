package graphqlshadow

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

var capturedResponseData = json.RawMessage(`{
  "torrentContent": {"search": {
    "totalCount": 1,
    "totalCountIsEstimate": false,
    "items": [{
      "id": "aa:movie:imdb:tt1"
    }],
    "aggregations": {}
  }}
}`)

func activeTestConfig() Config {
	return Config{
		Enabled:       true,
		Endpoint:      "http://dark.invalid/graphql",
		SampleRate:    1,
		Timeout:       time.Second,
		MaxConcurrent: 1,
	}
}

func operationContext(ctx context.Context, query, name string) context.Context {
	return graphql.WithOperationContext(ctx, &graphql.OperationContext{
		RawQuery:      query,
		OperationName: name,
		Variables:     map[string]any{"limit": 20},
	})
}

func responseHandler(calls *atomic.Int64) graphql.ResponseHandler {
	return func(context.Context) *graphql.Response {
		calls.Add(1)

		return &graphql.Response{Data: capturedResponseData}
	}
}

func runHook(
	ctx context.Context,
	hook *Hook,
	next graphql.ResponseHandler,
) *graphql.Response {
	ctx, capture := hook.Begin(ctx)
	response := hook.CaptureResponse(ctx, next)
	hook.Finish(ctx, capture)

	return response
}

// TestHookUnsafeOperationsMakeZeroRustCalls exercises the actual middleware
// boundary. Every primary Go handler runs exactly once, while mutations,
// subscriptions, parse failures, ambiguous documents, and unrelated queries
// make zero Rust calls.
func TestHookUnsafeOperationsMakeZeroRustCalls(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query string
		name  string
	}{
		{query: `mutation M { torrent { delete(infoHashes: ["aa"]) } }`, name: "M"},
		{query: `subscription S { version }`, name: "S"},
		{query: `this is not valid graphql`},
		{query: `query Q { version } mutation M { torrent { reprocess(infoHashes: []) } }`},
		{query: `query Q { version } mutation M { torrent { reprocess(infoHashes: []) } }`, name: "M"},
		{query: `{ version }`},
	}

	for _, test := range tests {
		rust := &spyExecutor{}
		hook := newHook(activeTestConfig(), rust, nil, nil)
		hook.run = func(f func()) { f() }
		hook.sample = func() float64 { return 0 }

		var goCalls atomic.Int64
		response := runHook(
			operationContext(context.Background(), test.query, test.name),
			hook,
			responseHandler(&goCalls),
		)

		if response == nil {
			t.Fatalf("query %q: primary response is nil", test.query)
		}

		if got := goCalls.Load(); got != 1 {
			t.Errorf("query %q: primary Go executions = %d, want exactly 1", test.query, got)
		}

		if got := rust.calls.Load(); got != 0 {
			t.Errorf("query %q: Rust calls = %d, want 0", test.query, got)
		}
	}
}

func TestHookUsesPreWriteResponseGenerationDuration(t *testing.T) {
	t.Parallel()

	rust := &spyExecutor{
		result:   GraphQLResult{IDs: []string{testInferID}, TotalCount: 1},
		duration: 7 * time.Millisecond,
	}
	metrics := NewMetrics()
	hook := newHook(activeTestConfig(), rust, metrics, nil)
	hook.run = func(f func()) { f() }
	hook.sample = func() float64 { return 0 }

	current := time.Unix(100, 0)
	hook.now = func() time.Time { return current }

	var goCalls atomic.Int64

	ctx, capture := hook.Begin(operationContext(context.Background(), comparableQuery, "Search"))
	response := hook.CaptureResponse(ctx, func(context.Context) *graphql.Response {
		goCalls.Add(1)
		current = current.Add(19 * time.Millisecond)

		return &graphql.Response{Data: capturedResponseData}
	})

	// A long delay after response generation but before Finish must not inflate
	// the Go reference duration.
	current = current.Add(time.Hour)

	hook.Finish(ctx, capture)

	if response == nil {
		t.Fatal("primary response is nil")
	}

	if got := goCalls.Load(); got != 1 {
		t.Fatalf("primary Go executions = %d, want exactly 1", got)
	}

	if got := rust.calls.Load(); got != 1 {
		t.Fatalf("Rust calls = %d, want 1", got)
	}

	if got := testutil.ToFloat64(metrics.comparisonsTotal); got != 1 {
		t.Fatalf("comparisons_total = %v, want 1", got)
	}

	metric := &dto.Metric{}
	if err := metrics.refLatency.Write(metric); err != nil {
		t.Fatalf("write reference latency metric: %v", err)
	}

	if got := metric.GetHistogram().GetSampleSum(); got != 0.019 {
		t.Errorf("reference handler duration sum = %v, want 0.019", got)
	}
}

func TestHookDefaultOffIsDirectPassthrough(t *testing.T) {
	t.Parallel()

	rust := &spyExecutor{}
	hook := newHook(NewDefaultConfig(), rust, nil, nil)
	hook.sample = func() float64 { panic("disabled hook drew a sample") }
	hook.now = func() time.Time { panic("disabled hook started a timer") }

	var goCalls atomic.Int64

	runHook(
		operationContext(context.Background(), comparableQuery, "Search"),
		hook,
		responseHandler(&goCalls),
	)

	if goCalls.Load() != 1 || rust.calls.Load() != 0 {
		t.Fatalf("Go/Rust calls = %d/%d, want 1/0", goCalls.Load(), rust.calls.Load())
	}
}

func TestHookSamplingMissMakesZeroRustCalls(t *testing.T) {
	t.Parallel()

	cfg := activeTestConfig()
	cfg.SampleRate = 0.1
	rust := &spyExecutor{}
	hook := newHook(cfg, rust, nil, nil)
	hook.sample = func() float64 { return 0.5 }

	var goCalls atomic.Int64

	runHook(
		operationContext(context.Background(), comparableQuery, "Search"),
		hook,
		responseHandler(&goCalls),
	)

	if goCalls.Load() != 1 || rust.calls.Load() != 0 {
		t.Fatalf("Go/Rust calls = %d/%d, want 1/0", goCalls.Load(), rust.calls.Load())
	}
}

func TestHookDetachesCancellationAndAddsHardTimeout(t *testing.T) {
	t.Parallel()

	var (
		wasCancelled bool
		hadDeadline  bool
	)

	rust := &spyExecutor{
		result: GraphQLResult{IDs: []string{testInferID}, TotalCount: 1},
		check: func(ctx context.Context) {
			wasCancelled = ctx.Err() != nil
			_, hadDeadline = ctx.Deadline()
		},
	}
	hook := newHook(activeTestConfig(), rust, nil, nil)
	hook.run = func(f func()) { f() }
	hook.sample = func() float64 { return 0 }

	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()

	var goCalls atomic.Int64

	runHook(
		operationContext(requestCtx, comparableQuery, "Search"),
		hook,
		responseHandler(&goCalls),
	)

	if wasCancelled {
		t.Error("Rust context inherited primary request cancellation")
	}

	if !hadDeadline {
		t.Error("Rust context has no hard timeout deadline")
	}
}

func TestHookSaturationDoesNotBlockOrStartSecondRustCall(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	rust := &spyExecutor{
		result: GraphQLResult{IDs: []string{testInferID}, TotalCount: 1},
		check: func(context.Context) {
			close(started)
			<-release
		},
	}
	metrics := NewMetrics()
	hook := newHook(activeTestConfig(), rust, metrics, nil)
	hook.sample = func() float64 { return 0 }

	var firstGoCalls atomic.Int64

	runHook(
		operationContext(context.Background(), comparableQuery, "Search"),
		hook,
		responseHandler(&firstGoCalls),
	)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first Rust comparison did not start")
	}

	var (
		secondGoCalls atomic.Int64
		secondDone    = make(chan struct{})
	)

	go func() {
		runHook(
			operationContext(context.Background(), comparableQuery, "Search"),
			hook,
			responseHandler(&secondGoCalls),
		)
		close(secondDone)
	}()

	select {
	case <-secondDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("saturated shadow hook blocked the served response")
	}

	if got := rust.calls.Load(); got != 1 {
		t.Errorf("Rust calls while saturated = %d, want 1", got)
	}

	if got := testutil.ToFloat64(metrics.saturatedTotal); got != 1 {
		t.Errorf("saturated_total = %v, want 1", got)
	}

	close(release)
}
