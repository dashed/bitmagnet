package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
)

var updateDHTReceiveDispatchParity = flag.Bool(
	"update-dht-receive-dispatch-parity",
	false,
	"rewrite the Rust DHT receive/dispatch parity fixture",
)

type receiveDispatchFixture struct {
	ID        string                  `json:"id"`
	Subsystem string                  `json:"subsystem"`
	Input     receiveDispatchInput    `json:"input"`
	Expected  receiveDispatchExpected `json:"expected"`
}

type receiveDispatchInput struct {
	WireHex         string              `json:"wireHex"`
	Source          receiveFixtureAddr  `json:"source"`
	PendingTIDHex   string              `json:"pendingTidHex,omitempty"`
	ExpectedSource  *receiveFixtureAddr `json:"expectedSource,omitempty"`
	DuplicateFilled bool                `json:"duplicateFilled,omitempty"`
}

type receiveFixtureAddr struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope,omitempty"`
}

type receiveDispatchExpected struct {
	GoOutcome          string `json:"goOutcome"`
	RustOutcome        string `json:"rustOutcome"`
	CanonicalWireHex   string `json:"canonicalWireHex,omitempty"`
	PendingAfter       bool   `json:"pendingAfter"`
	RustPendingAfter   bool   `json:"rustPendingAfter"`
	DeliveredWireHex   string `json:"deliveredWireHex,omitempty"`
	DeliveredSource    string `json:"deliveredSource,omitempty"`
	RegistryUnaffected bool   `json:"registryUnaffected,omitempty"`
}

type receiveDispatchScenario struct {
	id, wire, outcome, rustOutcome, pendingTID string
	source, expectedSource                     netip.AddrPort
	duplicateFilled                            bool
}

type receiveDispatchSocket struct {
	wire   []byte
	source netip.AddrPort
	cancel context.CancelFunc
	served bool
}

func (*receiveDispatchSocket) Open(netip.AddrPort) error { return nil }
func (*receiveDispatchSocket) Close() error              { return nil }
func (*receiveDispatchSocket) Send(netip.AddrPort, []byte) error {
	return nil
}
func (s *receiveDispatchSocket) Receive(buffer []byte) (int, netip.AddrPort, error) {
	if s.served {
		return 0, netip.AddrPort{}, context.Canceled
	}
	s.served = true
	copy(buffer, s.wire)
	s.cancel()
	return len(s.wire), s.source, nil
}

type receiveDispatchResponder struct {
	received chan dht.RecvMsg
}

func (r receiveDispatchResponder) Respond(_ context.Context, msg dht.RecvMsg) (dht.Return, error) {
	r.received <- msg
	return dht.Return{}, nil
}

func TestGenerateDHTReceiveDispatchParity(t *testing.T) {
	zero20 := "20:00000000000000000000"
	response := func(tid string, kind string) string {
		body := "1:rd2:id" + zero20 + "e"
		if kind == dht.YError {
			body = "1:eli201e4:oopse"
		}
		return "d" + body + "1:t" + lenString(tid) + ":" + tid + "1:y1:" + kind + "e"
	}
	query := "d1:ad2:id" + zero20 + "e1:q4:ping1:t2:Q11:v5:first1:y1:qe"
	remote4 := netip.MustParseAddrPort("1.2.3.4:6881")
	mapped4 := netip.MustParseAddrPort("[::ffff:1.2.3.4]:6881")
	native3 := netip.MustParseAddrPort("[fe80::1%3]:6881")
	native4 := netip.MustParseAddrPort("[fe80::1%4]:6881")
	scenarios := []receiveDispatchScenario{
		{id: "zero_length", source: remote4, outcome: "zero", rustOutcome: "zero"},
		{id: "malformed", wire: "d1:t2:X1", source: remote4, pendingTID: "P1", expectedSource: remote4, outcome: "decode_rejected", rustOutcome: "decode_rejected"},
		{id: "query_owned", wire: query, source: remote4, outcome: "query", rustOutcome: "query"},
		{id: "unsorted_duplicate_query", wire: "d1:y1:q1:t2:A11:t2:A21:q4:pinge", source: remote4, outcome: "query", rustOutcome: "query"},
		{id: "query_missing_q_and_a", wire: "d1:t2:M11:y1:qe", source: remote4, outcome: "query", rustOutcome: "query"},
		{id: "response_delivered", wire: response("R1", dht.YResponse), source: remote4, pendingTID: "R1", expectedSource: remote4, outcome: "response_delivered", rustOutcome: "response_delivered"},
		{id: "error_delivered", wire: response("E1", dht.YError), source: remote4, pendingTID: "E1", expectedSource: remote4, outcome: "error_delivered", rustOutcome: "error_delivered"},
		{id: "unknown_type_ignored", wire: "d1:t2:U11:y1:xe", source: remote4, pendingTID: "U1", expectedSource: remote4, outcome: "ignored", rustOutcome: "ignored"},
		{id: "missing_type_ignored", wire: "d1:t2:N1e", source: remote4, pendingTID: "N1", expectedSource: remote4, outcome: "ignored", rustOutcome: "ignored"},
		{id: "wrong_source", wire: response("W1", dht.YResponse), source: netip.MustParseAddrPort("1.2.3.4:6882"), pendingTID: "W1", expectedSource: remote4, outcome: "address_mismatch", rustOutcome: "address_mismatch"},
		{id: "duplicate", wire: response("D1", dht.YResponse), source: remote4, pendingTID: "D1", expectedSource: remote4, duplicateFilled: true, outcome: "duplicate", rustOutcome: "duplicate"},
		{id: "unknown_tid", wire: response("Z9", dht.YResponse), source: remote4, pendingTID: "K1", expectedSource: remote4, outcome: "unknown_tid", rustOutcome: "unknown_tid"},
		{id: "invalid_tid_hardening", wire: response("I", dht.YResponse), source: remote4, pendingTID: "I", expectedSource: remote4, outcome: "response_delivered", rustOutcome: "invalid_tid"},
		{id: "long_tid_hardening", wire: response("LNG", dht.YResponse), source: remote4, pendingTID: "LNG", expectedSource: remote4, outcome: "response_delivered", rustOutcome: "invalid_tid"},
		{id: "mapped_v4_source", wire: response("M1", dht.YResponse), source: remote4, pendingTID: "M1", expectedSource: mapped4, outcome: "response_delivered", rustOutcome: "response_delivered"},
		{id: "native_scope_match", wire: response("S1", dht.YResponse), source: native3, pendingTID: "S1", expectedSource: native3, outcome: "response_delivered", rustOutcome: "response_delivered"},
		{id: "native_scope_mismatch", wire: response("S2", dht.YResponse), source: native4, pendingTID: "S2", expectedSource: native3, outcome: "address_mismatch", rustOutcome: "address_mismatch"},
	}

	fixtures := make([]receiveDispatchFixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixtures = append(fixtures, runReceiveDispatchScenario(t, scenario))
	}
	writeOrCompareReceiveDispatchFixture(t, fixtures)
}

func runReceiveDispatchScenario(t *testing.T, scenario receiveDispatchScenario) receiveDispatchFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	socket := &receiveDispatchSocket{wire: []byte(scenario.wire), source: scenario.source, cancel: cancel}
	queryMessages := make(chan dht.RecvMsg, 1)
	collector := newPrometheusCollector()
	s := &server{
		socket:           socket,
		queries:          make(map[string]pendingQuery),
		responder:        receiveDispatchResponder{received: queryMessages},
		responderTimeout: time.Minute,
		responseDropped:  collector.responseDroppedTotal,
		logger:           zap.NewNop().Sugar(),
	}
	var pending chan dht.RecvMsg
	if scenario.pendingTID != "" {
		pending = make(chan dht.RecvMsg, 1)
		if scenario.duplicateFilled {
			pending <- dht.RecvMsg{}
		}
		s.queries[scenario.pendingTID] = pendingQuery{ch: pending, addr: scenario.expectedSource}
	}
	pendingLengthBefore := len(pending)

	s.read(ctx)
	var decoded dht.Msg
	decodeErr := bencode.Unmarshal([]byte(scenario.wire), &decoded)
	expected := receiveDispatchExpected{
		RustOutcome:      scenario.rustOutcome,
		PendingAfter:     scenario.pendingTID != "",
		RustPendingAfter: len(scenario.pendingTID) == 2,
	}
	goOutcome := ""
	switch {
	case len(scenario.wire) == 0:
		goOutcome = "zero"
	case decodeErr != nil:
		goOutcome = "decode_rejected"
	case decoded.Y == dht.YQuery:
		msg := receiveWithDeadline(t, queryMessages)
		expected.CanonicalWireHex = hex.EncodeToString(mustReceiveBencode(t, msg.Msg))
		expected.DeliveredSource = msg.From.String()
		goOutcome = "query"
	case decoded.Y != dht.YResponse && decoded.Y != dht.YError:
		goOutcome = "ignored"
	case scenario.pendingTID != decoded.T:
		waitReceiveEffect(t, func() bool {
			return testutil.ToFloat64(
				collector.responseDroppedTotal.WithLabelValues("unknown_tid"),
			) == 1
		})
		goOutcome = "unknown_tid"
	case !addrMatches(scenario.source, scenario.expectedSource):
		waitReceiveEffect(t, func() bool {
			return testutil.ToFloat64(
				collector.responseDroppedTotal.WithLabelValues("addr_mismatch"),
			) == 1
		})
		goOutcome = "address_mismatch"
	case scenario.duplicateFilled:
		before := len(pending)
		// The real read path has already spawned its handler. Invoke that exact
		// production handler synchronously as a completion probe: a full cap-1
		// channel must remain unchanged and the call must return.
		s.handleResponse(dht.RecvMsg{From: scenario.source, Msg: decoded})
		if before != 1 || len(pending) != 1 {
			t.Fatal("duplicate production handler changed the full delivery channel")
		}
		goOutcome = "duplicate"
	default:
		msg := receiveWithDeadline(t, pending)
		expected.DeliveredWireHex = hex.EncodeToString(mustReceiveBencode(t, msg.Msg))
		expected.DeliveredSource = msg.From.String()
		if decoded.Y == dht.YError {
			goOutcome = "error_delivered"
		} else {
			goOutcome = "response_delivered"
		}
	}
	if goOutcome != scenario.outcome {
		t.Fatalf("%s: observed %s, scenario expected %s", scenario.id, goOutcome, scenario.outcome)
	}
	expected.GoOutcome = goOutcome
	expected.RegistryUnaffected = goOutcome == "zero" || goOutcome == "decode_rejected" || goOutcome == "ignored"
	if expected.RegistryUnaffected {
		if len(queryMessages) != 0 ||
			len(pending) != pendingLengthBefore ||
			testutil.ToFloat64(collector.responseDroppedTotal.WithLabelValues("unknown_tid")) != 0 ||
			testutil.ToFloat64(collector.responseDroppedTotal.WithLabelValues("addr_mismatch")) != 0 {
			t.Fatal("non-dispatched datagram changed query or response-drop effects")
		}
	}
	if scenario.pendingTID != "" {
		s.mutex.Lock()
		_, expected.PendingAfter = s.queries[scenario.pendingTID]
		s.mutex.Unlock()
	}
	input := receiveDispatchInput{
		WireHex:         hex.EncodeToString([]byte(scenario.wire)),
		Source:          receiveAddr(scenario.source),
		PendingTIDHex:   hex.EncodeToString([]byte(scenario.pendingTID)),
		DuplicateFilled: scenario.duplicateFilled,
	}
	if scenario.pendingTID != "" {
		addr := receiveAddr(scenario.expectedSource)
		input.ExpectedSource = &addr
	}
	return receiveDispatchFixture{ID: scenario.id, Subsystem: "dht_receive_dispatch", Input: input, Expected: expected}
}

type receiveReuseSocket struct {
	wires   [][]byte
	source  netip.AddrPort
	cancel  context.CancelFunc
	current int
}

func (*receiveReuseSocket) Open(netip.AddrPort) error { return nil }
func (*receiveReuseSocket) Close() error              { return nil }
func (*receiveReuseSocket) Send(netip.AddrPort, []byte) error {
	return nil
}
func (s *receiveReuseSocket) Receive(buffer []byte) (int, netip.AddrPort, error) {
	if s.current == len(s.wires) {
		return 0, netip.AddrPort{}, context.Canceled
	}
	wire := s.wires[s.current]
	s.current++
	copy(buffer, wire)
	if s.current == len(s.wires) {
		s.cancel()
	}
	return len(wire), s.source, nil
}

func TestDHTReadOwnsMessagesAcrossBufferReuse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	socket := &receiveReuseSocket{
		wires: [][]byte{
			[]byte("d1:ad2:id20:00000000000000000000e1:q4:ping1:t2:Q11:v5:first1:y1:qe"),
			[]byte("d1:ad2:id20:00000000000000000000e1:q4:ping1:t2:Q21:v6:second1:y1:qe"),
		},
		source: netip.MustParseAddrPort("1.2.3.4:6881"),
		cancel: cancel,
	}
	received := make(chan dht.RecvMsg, 2)
	s := &server{
		socket:           socket,
		queries:          make(map[string]pendingQuery),
		responder:        receiveDispatchResponder{received: received},
		responderTimeout: time.Minute,
		logger:           zap.NewNop().Sugar(),
	}
	s.read(ctx)
	messages := map[string]string{}
	for range 2 {
		message := receiveWithDeadline(t, received)
		messages[message.Msg.T] = message.Msg.ClientID
	}
	if messages["Q1"] != "first" || messages["Q2"] != "second" {
		t.Fatalf("Go read retained borrowed buffer bytes: %#v", messages)
	}
}

func waitReceiveEffect(t *testing.T, effect func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !effect() {
		if time.Now().After(deadline) {
			t.Fatal("real Go read dispatch effect did not converge")
		}
		runtime.Gosched()
	}
}

func receiveWithDeadline(t *testing.T, messages <-chan dht.RecvMsg) dht.RecvMsg {
	t.Helper()
	select {
	case message := <-messages:
		return message
	case <-time.After(time.Second):
		t.Fatal("real Go read dispatch did not complete")
		return dht.RecvMsg{}
	}
}

func mustReceiveBencode(t *testing.T, message dht.Msg) []byte {
	t.Helper()
	wire, err := bencode.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func receiveAddr(addr netip.AddrPort) receiveFixtureAddr {
	scope := uint32(0)
	if addr.Addr().Zone() != "" {
		if _, err := fmt.Sscanf(addr.Addr().Zone(), "%d", &scope); err != nil {
			panic(err)
		}
	}
	return receiveFixtureAddr{IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: scope}
}

func lenString(value string) string { return strconv.Itoa(len(value)) }

func writeOrCompareReceiveDispatchFixture(t *testing.T, fixtures []receiveDispatchFixture) {
	t.Helper()
	var encoded []byte
	for _, fixture := range fixtures {
		line, err := json.Marshal(fixture)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, line...)
		encoded = append(encoded, '\n')
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve generator source")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../testdata/parity/dht/receive_dispatch.jsonl"))
	if *updateDHTReceiveDispatchParity {
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-receive-dispatch-parity: %v", err)
	}
	if !bytes.Equal(want, encoded) {
		t.Fatal("receive fixture is stale; rerun with -update-dht-receive-dispatch-parity")
	}
}
