package pathsearch

import (
	"context"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/search/tantivy/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestComposerSuggestReturnsServedValues(t *testing.T) {
	t.Parallel()

	l3 := &fakeL3{suggestResp: &pb.SuggestResponse{Suggestions: []*pb.Suggestion{
		{Value: "inception", Score: 20},
		{Value: "interstellar", Score: 10},
	}}}
	composer := newTestComposer(l3, &fakePG{})

	got, served, err := composer.Suggest(context.Background(), "movies/i", 2)
	if err != nil || !served {
		t.Fatalf("Suggest served=%v err=%v, want served=true err=nil", served, err)
	}

	want := []string{"inception", "interstellar"}
	if len(got) != len(want) {
		t.Fatalf("Suggest values = %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Suggest values = %v, want %v", got, want)
		}
	}

	if l3.suggestCalls != 1 || l3.suggestReq.GetPrefix() != "movies/i" || l3.suggestReq.GetLimit() != 2 {
		t.Fatalf("Suggest RPC calls=%d req=%+v, want one call with prefix and limit", l3.suggestCalls, l3.suggestReq)
	}
}

func TestComposerSuggestRPCErrorFallsBackSoftly(t *testing.T) {
	t.Parallel()

	l3 := &fakeL3{suggestErr: status.Error(codes.Unavailable, "prefix index unavailable")}
	composer := newTestComposer(l3, &fakePG{})

	got, served, err := composer.Suggest(context.Background(), "movies/i", 2)
	if err != nil || served || got != nil {
		t.Fatalf("Suggest values=%v served=%v err=%v, want nil false nil", got, served, err)
	}

	if l3.suggestCalls != 1 {
		t.Fatalf("Suggest RPC calls = %d, want 1", l3.suggestCalls)
	}
}

func TestComposerSuggestUnhealthyDoesNotCallRPC(t *testing.T) {
	t.Parallel()

	l3 := &fakeL3{suggestResp: &pb.SuggestResponse{Suggestions: []*pb.Suggestion{{Value: "unexpected"}}}}
	composer := NewComposer(
		l3,
		&fakePG{},
		ComposerConfig{},
		nil,
		WithHealthGate(func() bool { return false }),
	)

	got, served, err := composer.Suggest(context.Background(), "movies/i", 2)
	if err != nil || served || got != nil {
		t.Fatalf("Suggest values=%v served=%v err=%v, want nil false nil", got, served, err)
	}

	if l3.suggestCalls != 0 {
		t.Fatalf("unhealthy composer called Suggest RPC %d times, want 0", l3.suggestCalls)
	}
}

func TestNilComposerSuggestFallsBackSoftly(t *testing.T) {
	t.Parallel()

	var composer *Composer

	got, served, err := composer.Suggest(context.Background(), "movies/i", 2)
	if err != nil || served || got != nil {
		t.Fatalf("Suggest values=%v served=%v err=%v, want nil false nil", got, served, err)
	}
}

func TestComposerSuggestEmptyResponseIsAuthoritative(t *testing.T) {
	t.Parallel()

	l3 := &fakeL3{suggestResp: &pb.SuggestResponse{}}
	composer := newTestComposer(l3, &fakePG{})

	got, served, err := composer.Suggest(context.Background(), "movies/z", 3)
	if err != nil || !served {
		t.Fatalf("Suggest values=%v served=%v err=%v, want empty true nil", got, served, err)
	}

	if got == nil || len(got) != 0 {
		t.Fatalf("authoritative empty Suggest values = %#v, want non-nil empty slice", got)
	}
}
