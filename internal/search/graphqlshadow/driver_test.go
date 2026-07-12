package graphqlshadow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// spyExecutor / spyReference count calls so a test can assert the safety gate
// made ZERO downstream calls.
type spyExecutor struct {
	calls  atomic.Int64
	result GraphQLResult
	err    error
}

func (s *spyExecutor) Execute(_ context.Context, _ Request) (GraphQLResult, error) {
	s.calls.Add(1)

	return s.result, s.err
}

type spyReference struct {
	calls  atomic.Int64
	result GraphQLResult
	err    error
}

func (s *spyReference) Query(_ context.Context, _ Request) (GraphQLResult, error) {
	s.calls.Add(1)

	return s.result, s.err
}

// TestShadowOnceMutationMakesZeroReferenceCalls is THE load-bearing safety test:
// a mutation document must be hard-dropped before any execution, so it produces
// ZERO calls to the Rust executor and ZERO calls to the Go reference endpoint.
// A reference call on a mutation would double-apply a production side effect.
func TestShadowOnceMutationMakesZeroReferenceCalls(t *testing.T) {
	mutations := []Request{
		{Query: `mutation { torrent { delete(infoHashes: ["aabbccddeeff00112233445566778899aabbccdd"]) } }`},
		{Query: `mutation Del { torrent { deleteTags(infoHashes: ["aa"], tagNames: ["x"]) } }`},
		{Query: `mutation { queue { purgeJobs(queues: []) } }`},
		// A query paired with a mutation, operationName selecting the mutation.
		{
			Query:         `query Q { version } mutation M { torrent { reprocess(infoHashes: []) } }`,
			OperationName: "M",
		},
		// Ambiguous multi-op (no operationName) must also fail closed.
		{Query: `query Q { version } mutation M { torrent { delete(infoHashes: []) } }`},
		{Query: `subscription S { version }`},
		{Query: `this is not valid graphql`},
	}

	for _, req := range mutations {
		rust := &spyExecutor{}
		ref := &spyReference{}
		d := NewDriver(rust, ref, nil)

		outcome, cmp, err := d.ShadowOnce(context.Background(), req)
		if err != nil {
			t.Errorf("ShadowOnce(%q) returned error: %v", req.Query, err)
		}

		if outcome != OutcomeDropped {
			t.Errorf("ShadowOnce(%q) outcome = %q, want %q", req.Query, outcome, OutcomeDropped)
		}

		if cmp != nil {
			t.Errorf("ShadowOnce(%q) returned a comparison, want nil", req.Query)
		}

		if got := rust.calls.Load(); got != 0 {
			t.Errorf("ShadowOnce(%q) called Rust executor %d times, want 0", req.Query, got)
		}

		if got := ref.calls.Load(); got != 0 {
			t.Errorf("SAFETY VIOLATION: ShadowOnce(%q) called Go reference %d times, want 0 "+
				"(a mutation reaching the reference double-applies a prod side effect)", req.Query, got)
		}
	}
}

func TestShadowOnceQueryComparesBothSides(t *testing.T) {
	rust := &spyExecutor{result: GraphQLResult{IDs: []string{"a", "b"}, TotalCount: 2}}
	ref := &spyReference{result: GraphQLResult{IDs: []string{"a", "b"}, TotalCount: 2}}
	d := NewDriver(rust, ref, nil)

	outcome, cmp, err := d.ShadowOnce(context.Background(), Request{Query: `{ torrentContent { search { totalCount } } }`})
	if err != nil {
		t.Fatalf("ShadowOnce error: %v", err)
	}

	if outcome != OutcomeCompared {
		t.Fatalf("outcome = %q, want %q", outcome, OutcomeCompared)
	}

	if cmp == nil {
		t.Fatal("expected a comparison, got nil")
	}

	if !cmp.Top1Match || !cmp.TotalCountMatch || !cmp.AllFacetsMatch {
		t.Errorf("expected a clean match, got %+v", cmp)
	}

	if rust.calls.Load() != 1 || ref.calls.Load() != 1 {
		t.Errorf("expected exactly one call each; rust=%d ref=%d", rust.calls.Load(), ref.calls.Load())
	}
}

// TestShadowOnceRustErrorSkipsReference: a Rust failure must NOT trigger a
// reference call (no point paying reference cost, and it keeps a Rust bug from
// doubling reference load).
func TestShadowOnceRustErrorSkipsReference(t *testing.T) {
	rust := &spyExecutor{err: errors.New("boom")}
	ref := &spyReference{}
	d := NewDriver(rust, ref, nil)

	outcome, cmp, err := d.ShadowOnce(context.Background(), Request{Query: `{ version }`})
	if err == nil {
		t.Fatal("expected an error from the Rust failure")
	}

	if outcome != OutcomeRustError {
		t.Errorf("outcome = %q, want %q", outcome, OutcomeRustError)
	}

	if cmp != nil {
		t.Error("expected nil comparison on Rust error")
	}

	if ref.calls.Load() != 0 {
		t.Errorf("Go reference called %d times after a Rust error, want 0", ref.calls.Load())
	}
}

func TestShadowOnceReferenceError(t *testing.T) {
	rust := &spyExecutor{result: GraphQLResult{IDs: []string{"a"}}}
	ref := &spyReference{err: errors.New("upstream 500")}
	d := NewDriver(rust, ref, nil)

	outcome, cmp, err := d.ShadowOnce(context.Background(), Request{Query: `{ version }`})
	if err == nil {
		t.Fatal("expected an error from the reference failure")
	}

	if outcome != OutcomeReferenceError {
		t.Errorf("outcome = %q, want %q", outcome, OutcomeReferenceError)
	}

	if cmp != nil {
		t.Error("expected nil comparison on reference error")
	}
}
