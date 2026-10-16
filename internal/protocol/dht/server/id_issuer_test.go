package server

import (
	"context"
	"net/netip"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
)

func TestCryptoIDIssuerLength(t *testing.T) {
	t.Parallel()

	var issuer cryptoIDIssuer
	for range 1000 {
		id := issuer.Issue()
		if len(id) != 2 {
			t.Fatalf("expected 2-byte transaction ID, got %d bytes: %q", len(id), id)
		}
	}
}

func TestCryptoIDIssuerNonSequential(t *testing.T) {
	t.Parallel()

	var issuer cryptoIDIssuer

	const n = 64
	ids := make([]string, n)
	distinct := map[string]struct{}{}

	for i := range n {
		ids[i] = issuer.Issue()
		distinct[ids[i]] = struct{}{}
	}

	// A monotonic uvarint issuer (the old implementation) would yield a strictly
	// increasing, low-cardinality sequence. Crypto-random IDs must not.
	if len(distinct) < n/2 {
		t.Fatalf("expected high cardinality from random IDs, got %d distinct of %d", len(distinct), n)
	}

	strictlyIncreasing := true

	for i := 1; i < n; i++ {
		if ids[i] <= ids[i-1] {
			strictlyIncreasing = false
			break
		}
	}

	if strictlyIncreasing {
		t.Fatalf("transaction IDs were strictly increasing; expected non-sequential random values")
	}
}

// TestQueryUniqueInFlightTID drives Query with an issuer that first returns a
// transaction ID that collides with an in-flight query, then a unique one, and
// asserts the collision-retry registers both queries under distinct IDs.
func TestQueryUniqueInFlightTID(t *testing.T) {
	t.Parallel()

	// Sequence: A's only call -> "A"; B's first call -> "A" (collision) -> "B".
	issuer := &scriptedIssuer{ids: []string{"A", "A", "B"}}
	s, _, _ := newTestServer(issuer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addrA := netip.MustParseAddrPort("1.1.1.1:1")
	addrB := netip.MustParseAddrPort("2.2.2.2:2")

	_ = runQuery(ctx, s, addrA, dht.QPing)
	waitForQuery(t, s, "A")

	_ = runQuery(ctx, s, addrB, dht.QPing)
	waitForQuery(t, s, "B")

	s.mutex.Lock()
	_, okA := s.queries["A"]
	_, okB := s.queries["B"]
	n := len(s.queries)
	s.mutex.Unlock()

	if !okA || !okB {
		t.Fatalf("expected both A and B registered (okA=%v okB=%v)", okA, okB)
	}

	if n != 2 {
		t.Fatalf("expected 2 distinct in-flight queries, got %d", n)
	}
}
