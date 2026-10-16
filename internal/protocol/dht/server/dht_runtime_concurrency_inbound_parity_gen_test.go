package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	dhtresponder "github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/responder"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var updateDHTRuntimeConcurrencyInboundParity = flag.Bool(
	"update-dht-runtime-concurrency-inbound-parity",
	false,
	"rewrite the Rust DHT runtime concurrency/inbound parity fixture",
)

const dhtRuntimeConcurrencyInboundGoldenSHA256 = "d94b1429ab9f1b822df5496861ae90738b9d37b7da2bfa32fd6615915febb153"

type dhtRuntimeConcurrencyInboundFixture struct {
	ID        string                               `json:"id"`
	Subsystem string                               `json:"subsystem"`
	Runtime   dhtRuntimeConcurrencyInboundRuntime  `json:"runtime"`
	Input     dhtRuntimeConcurrencyInboundInput    `json:"input"`
	Expected  dhtRuntimeConcurrencyInboundExpected `json:"expected"`
}

type dhtRuntimeConcurrencyInboundRuntime struct {
	IntBits                 int    `json:"intBits"`
	Implementation          string `json:"implementation"`
	Coordination            string `json:"coordination"`
	ExistingWrapperEvidence string `json:"existingWrapperEvidence"`
	CompositionLimit        string `json:"compositionLimit"`
}

type dhtRuntimeConcurrencyInboundInput struct {
	Invocation    string                                     `json:"invocation"`
	Messages      []dhtRuntimeConcurrencyInboundInputMessage `json:"messages"`
	Pending       dhtRuntimeConcurrencyInboundPending        `json:"pending"`
	ResponderKind string                                     `json:"responderKind"`
	Limiter       dhtRuntimeConcurrencyInboundLimiter        `json:"limiter"`
	SocketKind    string                                     `json:"socketKind"`
}

type dhtRuntimeConcurrencyInboundInputMessage struct {
	Role     string                           `json:"role"`
	Delivery string                           `json:"delivery"`
	Source   dhtRuntimeConcurrencyInboundAddr `json:"source"`
	WireHex  string                           `json:"wireHex"`
}

type dhtRuntimeConcurrencyInboundPending struct {
	Present        bool                             `json:"present"`
	TIDHex         string                           `json:"tidHex"`
	ExpectedSource dhtRuntimeConcurrencyInboundAddr `json:"expectedSource"`
}

type dhtRuntimeConcurrencyInboundLimiter struct {
	Kind                  string  `json:"kind"`
	OverallLimitPerSecond float64 `json:"overallLimitPerSecond"`
	OverallBurst          int     `json:"overallBurst"`
	PerIPLimitPerSecond   float64 `json:"perIpLimitPerSecond"`
	PerIPBurst            int     `json:"perIpBurst"`
	PerIPCapacity         int     `json:"perIpCapacity"`
	PerIPTTLNanos         int64   `json:"perIpTtlNanos"`
}

type dhtRuntimeConcurrencyInboundExpected struct {
	ReceiveCalls                     int                                      `json:"receiveCalls"`
	ResponderCalls                   int                                      `json:"responderCalls"`
	LimiterCalls                     int                                      `json:"limiterCalls"`
	DelegateCalls                    int                                      `json:"delegateCalls"`
	TableEffectCalls                 int                                      `json:"tableEffectCalls"`
	PartialOrder                     dhtRuntimeConcurrencyInboundPartialOrder `json:"partialOrder"`
	Delivery                         dhtRuntimeConcurrencyInboundDelivery     `json:"delivery"`
	PendingEntryPresentAfterDelivery bool                                     `json:"pendingEntryPresentAfterDelivery"`
	HandlerDeadlinePresent           bool                                     `json:"handlerDeadlinePresent"`
	DenialErrorIdentityExact         bool                                     `json:"denialErrorIdentityExact"`
	DenialErrorSource                string                                   `json:"denialErrorSource"`
	Sends                            []dhtRuntimeConcurrencyInboundSend       `json:"sends"`
	Terminal                         string                                   `json:"terminal"`
}

type dhtRuntimeConcurrencyInboundPartialOrder struct {
	QuerySendEntered                         bool `json:"querySendEntered"`
	LaterResponseDeliveredBeforeSendRelease  bool `json:"laterResponseDeliveredBeforeSendRelease"`
	ReadAdvancedAfterScriptBeforeSendRelease bool `json:"readAdvancedAfterScriptBeforeSendRelease"`
	QuerySendCompletedBeforeRelease          bool `json:"querySendCompletedBeforeRelease"`
	QuerySendCompletedAfterRelease           bool `json:"querySendCompletedAfterRelease"`
}

type dhtRuntimeConcurrencyInboundDelivery struct {
	Present bool                             `json:"present"`
	Source  dhtRuntimeConcurrencyInboundAddr `json:"source"`
	WireHex string                           `json:"wireHex"`
}

type dhtRuntimeConcurrencyInboundSend struct {
	Destination dhtRuntimeConcurrencyInboundAddr     `json:"destination"`
	WireHex     string                               `json:"wireHex"`
	Envelope    dhtRuntimeConcurrencyInboundEnvelope `json:"envelope"`
}

type dhtRuntimeConcurrencyInboundEnvelope struct {
	TIDHex               string                               `json:"tidHex"`
	TypeHex              string                               `json:"typeHex"`
	Presence             dhtRuntimeConcurrencyInboundPresence `json:"presence"`
	ReturnIDHex          string                               `json:"returnIdHex"`
	Error                dhtRuntimeConcurrencyInboundError    `json:"error"`
	Canonical            bool                                 `json:"canonical"`
	TIDEchoed            bool                                 `json:"tidEchoed"`
	RequestFieldsCleared bool                                 `json:"requestFieldsCleared"`
}

type dhtRuntimeConcurrencyInboundPresence struct {
	Query     bool `json:"q"`
	Arguments bool `json:"a"`
	Return    bool `json:"r"`
	Error     bool `json:"e"`
	IP        bool `json:"ip"`
	ReadOnly  bool `json:"ro"`
	ClientID  bool `json:"v"`
}

type dhtRuntimeConcurrencyInboundError struct {
	Present    bool   `json:"present"`
	Code       int    `json:"code"`
	MessageHex string `json:"messageHex"`
}

type dhtRuntimeConcurrencyInboundAddr struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope"`
}

type dhtRuntimeConcurrencyInboundSocketMessage struct {
	wire []byte
	from netip.AddrPort
}

type dhtRuntimeConcurrencyInboundSocket struct {
	receiveMessages []dhtRuntimeConcurrencyInboundSocketMessage
	receiveCalls    atomic.Int64
	afterScript     chan struct{}
	receiveRelease  chan struct{}
	afterScriptOnce sync.Once

	sendEntered   chan struct{}
	sendRelease   chan struct{}
	sendCompleted chan struct{}
	sendOnce      sync.Once
	completeOnce  sync.Once

	mu           sync.Mutex
	destinations []netip.AddrPort
	wires        [][]byte
}

func (*dhtRuntimeConcurrencyInboundSocket) Open(netip.AddrPort) error { return nil }
func (*dhtRuntimeConcurrencyInboundSocket) Close() error              { return nil }

func (s *dhtRuntimeConcurrencyInboundSocket) Receive(buffer []byte) (int, netip.AddrPort, error) {
	call := int(s.receiveCalls.Add(1))
	if call <= len(s.receiveMessages) {
		message := s.receiveMessages[call-1]
		copy(buffer, message.wire)
		return len(message.wire), message.from, nil
	}
	if call != len(s.receiveMessages)+1 || s.afterScript == nil || s.receiveRelease == nil {
		panic(fmt.Sprintf("unexpected receive call %d", call))
	}
	s.afterScriptOnce.Do(func() { close(s.afterScript) })
	<-s.receiveRelease
	return 0, netip.AddrPort{}, errors.New("oracle receive released after cancellation")
}

func (s *dhtRuntimeConcurrencyInboundSocket) Send(destination netip.AddrPort, wire []byte) error {
	s.mu.Lock()
	s.destinations = append(s.destinations, destination)
	s.wires = append(s.wires, append([]byte(nil), wire...))
	s.mu.Unlock()
	if s.sendEntered != nil {
		s.sendOnce.Do(func() { close(s.sendEntered) })
		<-s.sendRelease
		s.completeOnce.Do(func() { close(s.sendCompleted) })
	}
	return nil
}

func (s *dhtRuntimeConcurrencyInboundSocket) sends() ([]netip.AddrPort, [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	destinations := append([]netip.AddrPort(nil), s.destinations...)
	wires := make([][]byte, len(s.wires))
	for index := range s.wires {
		wires[index] = append([]byte(nil), s.wires[index]...)
	}
	return destinations, wires
}

type dhtRuntimeConcurrencyInboundFixedResponder struct {
	returnValue     dht.Return
	calls           atomic.Int64
	deadlinePresent atomic.Bool
}

func (r *dhtRuntimeConcurrencyInboundFixedResponder) Respond(
	ctx context.Context,
	_ dht.RecvMsg,
) (dht.Return, error) {
	r.calls.Add(1)
	_, deadlinePresent := ctx.Deadline()
	r.deadlinePresent.Store(deadlinePresent)
	return r.returnValue, nil
}

// dhtRuntimeConcurrencyInboundLimiterAdapter is intentionally an oracle-only
// package-boundary seam. The production responderLimiter is private to the
// responder package, where responder_limiter.jsonl already freezes its
// per-IP-before-global ordering and no-delegate-on-denial behavior. This adapter
// composes the actual exported limiter and exact production denial sentinel with
// server.handleQuery, which is private to this package, without reflection or a
// production API change.
type dhtRuntimeConcurrencyInboundLimiterAdapter struct {
	limiter         dhtresponder.Limiter
	delegate        dhtresponder.Responder
	calls           atomic.Int64
	limiterCalls    atomic.Int64
	deadlinePresent atomic.Bool
	mu              sync.Mutex
	returnedError   error
}

func (r *dhtRuntimeConcurrencyInboundLimiterAdapter) Respond(
	ctx context.Context,
	msg dht.RecvMsg,
) (dht.Return, error) {
	r.calls.Add(1)
	_, deadlinePresent := ctx.Deadline()
	r.deadlinePresent.Store(deadlinePresent)
	r.limiterCalls.Add(1)
	if !r.limiter.Allow(msg.From.Addr()) {
		err := error(dhtresponder.ErrTooManyRequests)
		r.mu.Lock()
		r.returnedError = err
		r.mu.Unlock()
		return dht.Return{}, err
	}
	return r.delegate.Respond(ctx, msg)
}

func (r *dhtRuntimeConcurrencyInboundLimiterAdapter) errorIdentityExact() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.returnedError == dhtresponder.ErrTooManyRequests
}

type dhtRuntimeConcurrencyInboundEffectResponder struct {
	delegateCalls    atomic.Int64
	tableEffectCalls atomic.Int64
}

func (r *dhtRuntimeConcurrencyInboundEffectResponder) Respond(
	_ context.Context,
	_ dht.RecvMsg,
) (dht.Return, error) {
	r.delegateCalls.Add(1)
	r.tableEffectCalls.Add(1)
	return dht.Return{ID: dhtRuntimeConcurrencyInboundID(0xfe)}, nil
}

func TestGenerateDHTRuntimeConcurrencyInboundParity(t *testing.T) {
	if strconv.IntSize != 64 {
		t.Fatalf("DHT runtime concurrency/inbound parity requires 64-bit Go int, got %d", strconv.IntSize)
	}
	fixtures := []dhtRuntimeConcurrencyInboundFixture{
		runDHTRuntimeConcurrencyInboundBlockedReply(t),
		runDHTRuntimeConcurrencyInboundLimiterDenial(t),
	}
	assertDHTRuntimeConcurrencyInboundFixtureMatrix(t, fixtures)
	reconcileDHTRuntimeConcurrencyInboundFixtures(t, fixtures)
}

func runDHTRuntimeConcurrencyInboundBlockedReply(t *testing.T) dhtRuntimeConcurrencyInboundFixture {
	t.Helper()
	querySource := netip.MustParseAddrPort("192.0.2.1:6881")
	responseSource := netip.MustParseAddrPort("198.51.100.2:6882")
	query := dht.Msg{
		T: "Q1", Y: dht.YQuery, Q: dht.QPing,
		A: &dht.MsgArgs{ID: dhtRuntimeConcurrencyInboundID(0x11)},
	}
	response := dht.Msg{
		T: "R1", Y: dht.YResponse,
		R: &dht.Return{ID: dhtRuntimeConcurrencyInboundID(0x22)},
	}
	queryWire := dhtRuntimeConcurrencyInboundMarshal(t, query)
	responseWire := dhtRuntimeConcurrencyInboundMarshal(t, response)
	pendingChannel := make(chan dht.RecvMsg, 1)
	socket := &dhtRuntimeConcurrencyInboundSocket{
		receiveMessages: []dhtRuntimeConcurrencyInboundSocketMessage{
			{wire: queryWire, from: querySource},
			{wire: responseWire, from: responseSource},
		},
		afterScript: make(chan struct{}), receiveRelease: make(chan struct{}),
		sendEntered: make(chan struct{}), sendRelease: make(chan struct{}), sendCompleted: make(chan struct{}),
	}
	fixedResponder := &dhtRuntimeConcurrencyInboundFixedResponder{
		returnValue: dht.Return{ID: dhtRuntimeConcurrencyInboundID(0x33)},
	}
	srv := &server{
		socket: socket, queries: map[string]pendingQuery{
			"R1": {ch: pendingChannel, addr: responseSource},
		},
		responder: fixedResponder, responderTimeout: 5 * time.Second,
		logger: zap.NewNop().Sugar(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	readDone := make(chan any, 1)
	go func() {
		var recovered any
		defer func() { readDone <- recovered }()
		defer func() { recovered = recover() }()
		srv.read(ctx)
	}()

	dhtRuntimeConcurrencyInboundAwait(t, socket.sendEntered, "query reply Send entry")
	dhtRuntimeConcurrencyInboundAwait(t, socket.afterScript, "read loop advancement after scripted datagrams")
	delivered := dhtRuntimeConcurrencyInboundAwaitValue(t, pendingChannel, "correlated response delivery")
	sendCompletedBeforeRelease := dhtRuntimeConcurrencyInboundClosed(socket.sendCompleted)
	if sendCompletedBeforeRelease {
		t.Fatal("query reply Send completed before its release gate")
	}
	srv.mutex.Lock()
	_, pendingStillPresent := srv.queries["R1"]
	srv.mutex.Unlock()

	close(socket.sendRelease)
	dhtRuntimeConcurrencyInboundAwait(t, socket.sendCompleted, "query reply Send completion")
	cancel()
	close(socket.receiveRelease)
	if recovered := dhtRuntimeConcurrencyInboundAwaitValue(t, readDone, "read loop cancellation"); recovered != nil {
		t.Fatalf("read loop panicked: %#v", recovered)
	}

	destinations, wires := socket.sends()
	if got, want := int(socket.receiveCalls.Load()), 3; got != want {
		t.Fatalf("receive calls = %d, want %d", got, want)
	}
	if got, want := int(fixedResponder.calls.Load()), 1; got != want {
		t.Fatalf("responder calls = %d, want %d", got, want)
	}
	if len(wires) != 1 || len(destinations) != 1 {
		t.Fatalf("sends = %d/%d, want 1/1", len(destinations), len(wires))
	}
	if delivered.Msg.T != response.T || delivered.Msg.Y != response.Y || delivered.Msg.R == nil ||
		delivered.Msg.R.ID != response.R.ID || delivered.From != responseSource {
		t.Fatalf("delivered response changed: %#v", delivered)
	}
	return dhtRuntimeConcurrencyInboundFixture{
		ID: "blocked_query_reply_later_response_delivered", Subsystem: "dht_runtime_concurrency_inbound",
		Runtime: dhtRuntimeConcurrencyInboundRuntimeMetadata(),
		Input: dhtRuntimeConcurrencyInboundInput{
			Invocation: "server.read", Messages: []dhtRuntimeConcurrencyInboundInputMessage{
				{Role: "query", Delivery: "socket_receive_1", Source: dhtRuntimeConcurrencyInboundProjectAddr(querySource), WireHex: hex.EncodeToString(queryWire)},
				{Role: "correlated_response", Delivery: "socket_receive_2", Source: dhtRuntimeConcurrencyInboundProjectAddr(responseSource), WireHex: hex.EncodeToString(responseWire)},
			},
			Pending:       dhtRuntimeConcurrencyInboundPending{Present: true, TIDHex: hex.EncodeToString([]byte("R1")), ExpectedSource: dhtRuntimeConcurrencyInboundProjectAddr(responseSource)},
			ResponderKind: "fixed_success", Limiter: dhtRuntimeConcurrencyInboundLimiter{Kind: "none"},
			SocketKind: "scripted_receive_and_blocked_send",
		},
		Expected: dhtRuntimeConcurrencyInboundExpected{
			ReceiveCalls: 3, ResponderCalls: 1, LimiterCalls: 0, DelegateCalls: 0, TableEffectCalls: 0,
			PartialOrder: dhtRuntimeConcurrencyInboundPartialOrder{
				QuerySendEntered: true, LaterResponseDeliveredBeforeSendRelease: true,
				ReadAdvancedAfterScriptBeforeSendRelease: true,
				QuerySendCompletedBeforeRelease:          sendCompletedBeforeRelease,
				QuerySendCompletedAfterRelease:           true,
			},
			Delivery:                         dhtRuntimeConcurrencyInboundDelivery{Present: true, Source: dhtRuntimeConcurrencyInboundProjectAddr(delivered.From), WireHex: hex.EncodeToString(responseWire)},
			PendingEntryPresentAfterDelivery: pendingStillPresent,
			HandlerDeadlinePresent:           fixedResponder.deadlinePresent.Load(),
			DenialErrorIdentityExact:         false, DenialErrorSource: "",
			Sends:    dhtRuntimeConcurrencyInboundProjectSends(t, destinations, wires, query.T),
			Terminal: "read_returned_after_cancel",
		},
	}
}

func runDHTRuntimeConcurrencyInboundLimiterDenial(t *testing.T) dhtRuntimeConcurrencyInboundFixture {
	t.Helper()
	source := netip.MustParseAddrPort("203.0.113.9:6999")
	query := dht.Msg{
		T: "L1", Y: dht.YQuery, Q: dht.QPing,
		A: &dht.MsgArgs{ID: dhtRuntimeConcurrencyInboundID(0x44)},
	}
	queryWire := dhtRuntimeConcurrencyInboundMarshal(t, query)
	effectResponder := &dhtRuntimeConcurrencyInboundEffectResponder{}
	const perIPTTL = time.Hour
	limiterAdapter := &dhtRuntimeConcurrencyInboundLimiterAdapter{
		limiter:  dhtresponder.NewLimiter(rate.Limit(0), 0, rate.Limit(0), 0, 1, perIPTTL),
		delegate: effectResponder,
	}
	socket := &dhtRuntimeConcurrencyInboundSocket{}
	srv := &server{
		socket: socket, queries: make(map[string]pendingQuery), responder: limiterAdapter,
		responderTimeout: 5 * time.Second, logger: zap.NewNop().Sugar(),
	}
	srv.handleQuery(context.Background(), dht.RecvMsg{Msg: query, From: source})
	destinations, wires := socket.sends()
	if len(wires) != 1 || len(destinations) != 1 {
		t.Fatalf("limiter denial sends = %d/%d, want 1/1", len(destinations), len(wires))
	}
	if got, want := int(limiterAdapter.calls.Load()), 1; got != want {
		t.Fatalf("limiter adapter calls = %d, want %d", got, want)
	}
	if got, want := int(limiterAdapter.limiterCalls.Load()), 1; got != want {
		t.Fatalf("limiter calls = %d, want %d", got, want)
	}
	if effectResponder.delegateCalls.Load() != 0 || effectResponder.tableEffectCalls.Load() != 0 {
		t.Fatalf("denied query reached delegate/table: delegate=%d table=%d", effectResponder.delegateCalls.Load(), effectResponder.tableEffectCalls.Load())
	}
	if !limiterAdapter.errorIdentityExact() {
		t.Fatal("limiter denial did not preserve exact responder.ErrTooManyRequests value")
	}
	return dhtRuntimeConcurrencyInboundFixture{
		ID: "limiter_denial_exact_response_wire", Subsystem: "dht_runtime_concurrency_inbound",
		Runtime: dhtRuntimeConcurrencyInboundRuntimeMetadata(),
		Input: dhtRuntimeConcurrencyInboundInput{
			Invocation: "server.handleQuery", Messages: []dhtRuntimeConcurrencyInboundInputMessage{
				{Role: "query", Delivery: "direct_handle_query", Source: dhtRuntimeConcurrencyInboundProjectAddr(source), WireHex: hex.EncodeToString(queryWire)},
			},
			Pending:       dhtRuntimeConcurrencyInboundPending{Present: false},
			ResponderKind: "actual_limiter_exact_denial_adapter",
			Limiter: dhtRuntimeConcurrencyInboundLimiter{
				Kind: "responder.NewLimiter", OverallLimitPerSecond: 0, OverallBurst: 0,
				PerIPLimitPerSecond: 0, PerIPBurst: 0, PerIPCapacity: 1, PerIPTTLNanos: int64(perIPTTL),
			},
			SocketKind: "capture_send_success",
		},
		Expected: dhtRuntimeConcurrencyInboundExpected{
			ReceiveCalls: 0, ResponderCalls: 1, LimiterCalls: 1,
			DelegateCalls: int(effectResponder.delegateCalls.Load()), TableEffectCalls: int(effectResponder.tableEffectCalls.Load()),
			PartialOrder: dhtRuntimeConcurrencyInboundPartialOrder{}, Delivery: dhtRuntimeConcurrencyInboundDelivery{},
			PendingEntryPresentAfterDelivery: false,
			HandlerDeadlinePresent:           limiterAdapter.deadlinePresent.Load(),
			DenialErrorIdentityExact:         limiterAdapter.errorIdentityExact(),
			DenialErrorSource:                "responder.ErrTooManyRequests",
			Sends:                            dhtRuntimeConcurrencyInboundProjectSends(t, destinations, wires, query.T),
			Terminal:                         "handle_query_returned_after_send",
		},
	}
}

func dhtRuntimeConcurrencyInboundRuntimeMetadata() dhtRuntimeConcurrencyInboundRuntime {
	return dhtRuntimeConcurrencyInboundRuntime{
		IntBits: 64, Implementation: "go_production_paths_with_oracle_only_gates",
		Coordination:            "channels_only_no_sleeps",
		ExistingWrapperEvidence: "testdata/parity/dht/responder_limiter.jsonl#outer_denial_and_delegate_effects",
		CompositionLimit:        "private responderLimiter is proven separately; denial row composes actual exported limiter and exact denial sentinel through private server.handleQuery",
	}
}

func dhtRuntimeConcurrencyInboundMarshal(t *testing.T, msg dht.Msg) []byte {
	t.Helper()
	wire, err := bencode.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal DHT input: %v", err)
	}
	return wire
}

func dhtRuntimeConcurrencyInboundProjectSends(
	t *testing.T,
	destinations []netip.AddrPort,
	wires [][]byte,
	requestTID string,
) []dhtRuntimeConcurrencyInboundSend {
	t.Helper()
	if len(destinations) != len(wires) {
		t.Fatalf("destination/wire length mismatch: %d/%d", len(destinations), len(wires))
	}
	sends := make([]dhtRuntimeConcurrencyInboundSend, 0, len(wires))
	for index := range wires {
		sends = append(sends, dhtRuntimeConcurrencyInboundSend{
			Destination: dhtRuntimeConcurrencyInboundProjectAddr(destinations[index]),
			WireHex:     hex.EncodeToString(wires[index]),
			Envelope:    dhtRuntimeConcurrencyInboundProjectEnvelope(t, wires[index], requestTID),
		})
	}
	return sends
}

func dhtRuntimeConcurrencyInboundProjectEnvelope(
	t *testing.T,
	wire []byte,
	requestTID string,
) dhtRuntimeConcurrencyInboundEnvelope {
	t.Helper()
	var raw map[string]interface{}
	if err := bencode.Unmarshal(wire, &raw); err != nil {
		t.Fatalf("decode sent wire generically: %v", err)
	}
	canonical, err := bencode.Marshal(raw)
	if err != nil {
		t.Fatalf("re-encode sent wire generically: %v", err)
	}
	var decoded dht.Msg
	if err := bencode.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("decode sent wire as DHT message: %v", err)
	}
	presence := dhtRuntimeConcurrencyInboundPresence{}
	_, presence.Query = raw["q"]
	_, presence.Arguments = raw["a"]
	_, presence.Return = raw["r"]
	_, presence.Error = raw["e"]
	_, presence.IP = raw["ip"]
	_, presence.ReadOnly = raw["ro"]
	_, presence.ClientID = raw["v"]
	envelope := dhtRuntimeConcurrencyInboundEnvelope{
		TIDHex: hex.EncodeToString([]byte(decoded.T)), TypeHex: hex.EncodeToString([]byte(decoded.Y)),
		Presence: presence, Canonical: bytes.Equal(canonical, wire), TIDEchoed: decoded.T == requestTID,
		RequestFieldsCleared: !presence.Query && !presence.Arguments && !presence.IP && !presence.ReadOnly && !presence.ClientID,
	}
	if decoded.R != nil {
		envelope.ReturnIDHex = hex.EncodeToString(decoded.R.ID[:])
	}
	if decoded.E != nil {
		envelope.Error = dhtRuntimeConcurrencyInboundError{
			Present: true, Code: decoded.E.Code, MessageHex: hex.EncodeToString([]byte(decoded.E.Msg)),
		}
	}
	if decoded.Y != dht.YResponse || !envelope.Canonical || !envelope.TIDEchoed || !envelope.RequestFieldsCleared {
		t.Fatalf("sent envelope invariant changed: %#v", envelope)
	}
	return envelope
}

func dhtRuntimeConcurrencyInboundProjectAddr(addr netip.AddrPort) dhtRuntimeConcurrencyInboundAddr {
	scope := uint32(0)
	if addr.Addr().Zone() != "" {
		parsed, err := strconv.ParseUint(addr.Addr().Zone(), 10, 32)
		if err != nil {
			panic(err)
		}
		scope = uint32(parsed)
	}
	return dhtRuntimeConcurrencyInboundAddr{IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: scope}
}

func dhtRuntimeConcurrencyInboundID(last byte) protocol.ID {
	var id protocol.ID
	id[len(id)-1] = last
	return id
}

func dhtRuntimeConcurrencyInboundAwait(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func dhtRuntimeConcurrencyInboundAwaitValue[T any](t *testing.T, ch <-chan T, description string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}

func dhtRuntimeConcurrencyInboundClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func assertDHTRuntimeConcurrencyInboundFixtureMatrix(
	t *testing.T,
	fixtures []dhtRuntimeConcurrencyInboundFixture,
) {
	t.Helper()
	wantIDs := []string{
		"blocked_query_reply_later_response_delivered",
		"limiter_denial_exact_response_wire",
	}
	if len(fixtures) != len(wantIDs) {
		t.Fatalf("fixture rows = %d, want %d", len(fixtures), len(wantIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != wantIDs[index] {
			t.Fatalf("fixture %d ID = %q, want %q", index, fixture.ID, wantIDs[index])
		}
		if fixture.Subsystem != "dht_runtime_concurrency_inbound" || fixture.Runtime.IntBits != 64 {
			t.Fatalf("%s: unstable subsystem/runtime metadata", fixture.ID)
		}
	}
	if !fixtures[0].Expected.PartialOrder.LaterResponseDeliveredBeforeSendRelease ||
		fixtures[0].Expected.PartialOrder.QuerySendCompletedBeforeRelease ||
		!fixtures[0].Expected.PartialOrder.QuerySendCompletedAfterRelease ||
		!fixtures[0].Expected.PendingEntryPresentAfterDelivery {
		t.Fatalf("blocked-reply partial order changed: %#v", fixtures[0].Expected)
	}
	denial := fixtures[1].Expected
	if denial.DelegateCalls != 0 || denial.TableEffectCalls != 0 || !denial.DenialErrorIdentityExact || len(denial.Sends) != 1 {
		t.Fatalf("limiter denial effects changed: %#v", denial)
	}
	if errorEnvelope := denial.Sends[0].Envelope; !errorEnvelope.Presence.Error || errorEnvelope.Presence.Return ||
		!errorEnvelope.Error.Present || errorEnvelope.Error.Code != 201 ||
		errorEnvelope.Error.MessageHex != hex.EncodeToString([]byte("too many requests")) {
		t.Fatalf("limiter denial envelope changed: %#v", errorEnvelope)
	}
	assertDHTRuntimeConcurrencyInboundIndependentGoldens(t, fixtures)
}

func assertDHTRuntimeConcurrencyInboundIndependentGoldens(
	t *testing.T,
	fixtures []dhtRuntimeConcurrencyInboundFixture,
) {
	t.Helper()
	type goldens struct {
		inputs []string
		sends  []string
	}
	want := map[string]goldens{
		"blocked_query_reply_later_response_delivered": {
			inputs: []string{"64313a6164323a696432303a000000000000000000000000000000000000001165313a71343a70696e67313a74323a5131313a79313a7165", "64313a7264323a696432303a000000000000000000000000000000000000002265313a74323a5231313a79313a7265"},
			sends:  []string{"64313a7264323a696432303a000000000000000000000000000000000000003365313a74323a5131313a79313a7265"},
		},
		"limiter_denial_exact_response_wire": {
			inputs: []string{"64313a6164323a696432303a000000000000000000000000000000000000004465313a71343a70696e67313a74323a4c31313a79313a7165"},
			sends:  []string{"64313a656c693230316531373a746f6f206d616e7920726571756573747365313a74323a4c31313a79313a7265"},
		},
	}
	for _, fixture := range fixtures {
		golden, ok := want[fixture.ID]
		if !ok {
			t.Fatalf("no independent golden for fixture %q", fixture.ID)
		}
		if len(fixture.Input.Messages) != len(golden.inputs) || len(fixture.Expected.Sends) != len(golden.sends) {
			t.Fatalf("%s: golden shape changed", fixture.ID)
		}
		for index, message := range fixture.Input.Messages {
			if message.WireHex != golden.inputs[index] {
				t.Fatalf("%s input %d golden changed:\n got %s\nwant %s", fixture.ID, index, message.WireHex, golden.inputs[index])
			}
		}
		for index, send := range fixture.Expected.Sends {
			if send.WireHex != golden.sends[index] {
				t.Fatalf("%s send %d golden changed:\n got %s\nwant %s", fixture.ID, index, send.WireHex, golden.sends[index])
			}
		}
	}
}

func reconcileDHTRuntimeConcurrencyInboundFixtures(
	t *testing.T,
	fixtures []dhtRuntimeConcurrencyInboundFixture,
) {
	t.Helper()
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	for _, fixture := range fixtures {
		if err := encoder.Encode(fixture); err != nil {
			t.Fatal(err)
		}
	}
	generated := encoded.Bytes()
	digest := sha256.Sum256(generated)
	digestHex := hex.EncodeToString(digest[:])
	if dhtRuntimeConcurrencyInboundGoldenSHA256 != "TODO" && digestHex != dhtRuntimeConcurrencyInboundGoldenSHA256 {
		t.Fatalf("generated runtime concurrency/inbound fixture digest = %s, want %s", digestHex, dhtRuntimeConcurrencyInboundGoldenSHA256)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve generator source")
	}
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source), "../../../../testdata/parity/dht/dht_runtime_concurrency_inbound.jsonl",
	))
	if *updateDHTRuntimeConcurrencyInboundParity {
		if err := os.WriteFile(path, generated, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-runtime-concurrency-inbound-parity: %v", err)
	}
	if !bytes.Equal(want, generated) {
		t.Fatal("DHT runtime concurrency/inbound fixture is stale; rerun with -update-dht-runtime-concurrency-inbound-parity")
	}
}
