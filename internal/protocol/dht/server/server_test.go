package server

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
)

// fakeSocket is a deterministic Socket seam: it records Send targets and never
// produces inbound packets (read() is not started in these tests).
type fakeSocket struct {
	mu   sync.Mutex
	sent []sentPacket
	recv chan struct{}
}

type sentPacket struct {
	addr netip.AddrPort
	data []byte
}

func newFakeSocket() *fakeSocket {
	return &fakeSocket{recv: make(chan struct{})}
}

func (*fakeSocket) Open(netip.AddrPort) error { return nil }

func (f *fakeSocket) Close() error {
	close(f.recv)
	return nil
}

func (f *fakeSocket) Send(addr netip.AddrPort, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	cp := make([]byte, len(data))
	copy(cp, data)
	f.sent = append(f.sent, sentPacket{addr: addr, data: cp})

	return nil
}

func (f *fakeSocket) Receive([]byte) (int, netip.AddrPort, error) {
	// Never exercised: read() is not started by the tests. Block until Close.
	<-f.recv
	return 0, netip.AddrPort{}, context.Canceled
}

// scriptedIssuer returns a fixed sequence of transaction IDs, repeating the
// last entry once exhausted. Used to force happy-path and collision scenarios.
type scriptedIssuer struct {
	mu  sync.Mutex
	ids []string
	i   int
}

func (s *scriptedIssuer) Issue() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.ids[s.i]

	if s.i < len(s.ids)-1 {
		s.i++
	}

	return id
}

func newTestServer(issuer IDIssuer) (*server, *fakeSocket, prometheusCollector) {
	sock := newFakeSocket()
	coll := newPrometheusCollector()
	s := &server{
		stopped:          make(chan struct{}),
		socket:           sock,
		queryTimeout:     200 * time.Millisecond,
		queries:          make(map[string]pendingQuery),
		responderTimeout: time.Second,
		idIssuer:         issuer,
		responseDropped:  coll.responseDroppedTotal,
		logger:           zap.NewNop().Sugar(),
	}

	return s, sock, coll
}

type queryResult struct {
	msg dht.RecvMsg
	err error
}

func runQuery(ctx context.Context, s *server, addr netip.AddrPort, q string) chan queryResult {
	resCh := make(chan queryResult, 1)
	go func() {
		r, err := s.Query(ctx, addr, q, dht.MsgArgs{})
		resCh <- queryResult{msg: r, err: err}
	}()

	return resCh
}

func waitForQuery(t *testing.T, s *server, tid string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mutex.Lock()
		_, ok := s.queries[tid]
		s.mutex.Unlock()

		if ok {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatalf("query %q was not registered in time", tid)
}

func response(from netip.AddrPort, tid string) dht.RecvMsg {
	return dht.RecvMsg{
		From: from,
		Msg: dht.Msg{
			T: tid,
			Y: dht.YResponse,
			R: &dht.Return{},
		},
	}
}

func TestQueryHappyPath(t *testing.T) {
	t.Parallel()

	s, _, _ := newTestServer(&scriptedIssuer{ids: []string{"tid1"}})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr := netip.MustParseAddrPort("1.2.3.4:6881")
	resCh := runQuery(ctx, s, addr, dht.QPing)
	waitForQuery(t, s, "tid1")

	// Response from the queried address (Is4 == Is4): must be delivered.
	s.handleResponse(response(addr, "tid1"))

	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("expected query to succeed, got error: %v", res.err)
		}

		if res.msg.Msg.R == nil {
			t.Fatalf("expected return data in delivered response")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("query did not return after a matching response")
	}
}

func TestQueryDropsSpoofedAddr(t *testing.T) {
	t.Parallel()

	s, _, _ := newTestServer(&scriptedIssuer{ids: []string{"tid2"}})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queried := netip.MustParseAddrPort("1.2.3.4:6881")
	attacker := netip.MustParseAddrPort("9.9.9.9:6881")

	resCh := runQuery(ctx, s, queried, dht.QPing)
	waitForQuery(t, s, "tid2")

	// Matching TID but a different source address: off-path injection, dropped.
	s.handleResponse(response(attacker, "tid2"))

	select {
	case res := <-resCh:
		if res.err == nil {
			t.Fatal("expected query to time out after dropping spoofed response")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("query did not return (should have timed out)")
	}

	if got := testutil.ToFloat64(s.responseDropped.WithLabelValues("addr_mismatch")); got != 1 {
		t.Fatalf("expected addr_mismatch drop counter == 1, got %v", got)
	}
}

func TestQueryDropsWrongPort(t *testing.T) {
	t.Parallel()

	s, _, _ := newTestServer(&scriptedIssuer{ids: []string{"tidp"}})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queried := netip.MustParseAddrPort("1.2.3.4:6881")
	// Same IP, different port: addrMatches gates on port, so this is a spoof.
	wrongPort := netip.MustParseAddrPort("1.2.3.4:6882")

	resCh := runQuery(ctx, s, queried, dht.QPing)
	waitForQuery(t, s, "tidp")

	s.handleResponse(response(wrongPort, "tidp"))

	select {
	case res := <-resCh:
		if res.err == nil {
			t.Fatal("expected query to time out after dropping wrong-port response")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("query did not return (should have timed out)")
	}

	if got := testutil.ToFloat64(s.responseDropped.WithLabelValues("addr_mismatch")); got != 1 {
		t.Fatalf("expected addr_mismatch drop counter == 1, got %v", got)
	}
}

func TestQueryDeliversErrorResponse(t *testing.T) {
	t.Parallel()

	s, _, _ := newTestServer(&scriptedIssuer{ids: []string{"tide"}})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr := netip.MustParseAddrPort("1.2.3.4:6881")
	resCh := runQuery(ctx, s, addr, dht.QPing)
	waitForQuery(t, s, "tide")

	// A legitimate KRPC error from the correct address: must pass the addr
	// check and be delivered, surfacing as a non-nil error via the YError branch.
	s.handleResponse(dht.RecvMsg{
		From: addr,
		Msg: dht.Msg{
			T: "tide",
			Y: dht.YError,
			E: &dht.Error{Code: dht.ErrorCodeServerError, Msg: "boom"},
		},
	})

	select {
	case res := <-resCh:
		if res.err == nil {
			t.Fatal("expected YError response to surface as a non-nil error")
		}

		var dhtErr *dht.Error
		if !errors.As(res.err, &dhtErr) {
			t.Fatalf("expected a *dht.Error, got %T: %v", res.err, res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("error response from the correct addr was not delivered")
	}

	// A genuine, addr-verified error is not a drop.
	if got := testutil.ToFloat64(s.responseDropped.WithLabelValues("addr_mismatch")); got != 0 {
		t.Fatalf("expected no addr_mismatch drops, got %v", got)
	}
}

func TestHandleResponseUnknownTID(t *testing.T) {
	t.Parallel()

	s, _, _ := newTestServer(&scriptedIssuer{ids: []string{"x"}})
	addr := netip.MustParseAddrPort("1.2.3.4:6881")

	// No in-flight query with this TID: must not panic, must count.
	s.handleResponse(response(addr, "not-registered"))

	if got := testutil.ToFloat64(s.responseDropped.WithLabelValues("unknown_tid")); got != 1 {
		t.Fatalf("expected unknown_tid drop counter == 1, got %v", got)
	}
}

func TestQuery4in6Normalization(t *testing.T) {
	t.Parallel()

	v4 := netip.MustParseAddr("1.2.3.4")

	v4in6 := netip.AddrFrom16(v4.As16()) // Is4In6 form of the same address
	if !v4in6.Is4In6() {
		t.Fatalf("expected v4in6 to be Is4In6")
	}

	const port = 6881

	cases := []struct {
		name    string
		queried netip.AddrPort
		from    netip.AddrPort
	}{
		{
			name:    "queried 4in6, response from v4",
			queried: netip.AddrPortFrom(v4in6, port),
			from:    netip.AddrPortFrom(v4, port),
		},
		{
			name:    "queried v4, response from 4in6",
			queried: netip.AddrPortFrom(v4, port),
			from:    netip.AddrPortFrom(v4in6, port),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s, _, _ := newTestServer(&scriptedIssuer{ids: []string{"tid"}})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			resCh := runQuery(ctx, s, tc.queried, dht.QPing)
			waitForQuery(t, s, "tid")

			s.handleResponse(response(tc.from, "tid"))

			select {
			case res := <-resCh:
				if res.err != nil {
					t.Fatalf("expected delivery across 4-in-6 forms, got error: %v", res.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("query did not return; 4-in-6 normalization failed")
			}
		})
	}
}

func TestHandleResponseNonBlockingDuplicate(t *testing.T) {
	t.Parallel()

	s, _, _ := newTestServer(&scriptedIssuer{ids: []string{"t"}})
	addr := netip.MustParseAddrPort("1.2.3.4:6881")

	ch := make(chan dht.RecvMsg, 1)

	s.mutex.Lock()
	s.queries["t"] = pendingQuery{ch: ch, addr: addr}
	s.mutex.Unlock()

	msg := response(addr, "t")
	s.handleResponse(msg) // fills the cap-1 channel

	// A second accepted response must not block on the full channel.
	done := make(chan struct{})
	go func() {
		s.handleResponse(msg)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleResponse blocked on a duplicate accepted response")
	}

	if len(ch) != 1 {
		t.Fatalf("expected exactly 1 buffered message, got %d", len(ch))
	}
}
