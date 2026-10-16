package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var updateDHTPingFindNodeDriverParity = flag.Bool(
	"update-dht-ping-find-node-driver-parity",
	false,
	"rewrite the Rust DHT ping/find-node one-datagram driver fixture",
)

type pingFindNodeDriverFixture struct {
	ID        string                     `json:"id"`
	Subsystem string                     `json:"subsystem"`
	Input     pingFindNodeDriverInput    `json:"input"`
	Expected  pingFindNodeDriverExpected `json:"expected"`
}

type pingFindNodeDriverInput struct {
	WireHex        string                   `json:"wireHex"`
	Source         pingFindNodeDriverAddr   `json:"source"`
	Origin         string                   `json:"origin"`
	Nodes          []pingFindNodeDriverNode `json:"nodes,omitempty"`
	PendingTIDHex  string                   `json:"pendingTidHex,omitempty"`
	ExpectedSource *pingFindNodeDriverAddr  `json:"expectedSource,omitempty"`
	SendFails      bool                     `json:"sendFails,omitempty"`
}

type pingFindNodeDriverAddr struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope,omitempty"`
}

type pingFindNodeDriverNode struct {
	ID   string                 `json:"id"`
	Addr pingFindNodeDriverAddr `json:"addr"`
}

type pingFindNodeDriverExpected struct {
	GoOutcome          string                  `json:"goOutcome"`
	RustOutcome        string                  `json:"rustOutcome"`
	Events             []string                `json:"events"`
	RustEvents         []string                `json:"rustEvents"`
	Destination        *pingFindNodeDriverAddr `json:"destination,omitempty"`
	WireHex            string                  `json:"wireHex,omitempty"`
	SendCalls          int                     `json:"sendCalls"`
	ReceiveCalls       int                     `json:"receiveCalls"`
	PendingAfter       bool                    `json:"pendingAfter,omitempty"`
	IntentionalPartial bool                    `json:"intentionalPartial,omitempty"`
	SendFailureLogged  bool                    `json:"sendFailureLogged,omitempty"`
}

type pingFindNodeDriverScenario struct {
	id, rustOutcome string
	wire            []byte
	source          netip.AddrPort
	origin          protocol.ID
	nodes           []dht.NodeInfo
	pendingTID      string
	expectedSource  netip.AddrPort
	sendFails       bool
	response        dht.Return
	responseErr     error
}

type pingFindNodeDriverSocket struct {
	mu           sync.Mutex
	wire         []byte
	source       netip.AddrPort
	cancel       context.CancelFunc
	served       bool
	receiveCalls int
	sends        []pingFindNodeDriverSent
	events       []string
	sendErr      error
	sendObserved chan struct{}
}

type pingFindNodeDriverSent struct {
	destination netip.AddrPort
	wire        []byte
}

func (*pingFindNodeDriverSocket) Open(netip.AddrPort) error { return nil }
func (*pingFindNodeDriverSocket) Close() error              { return nil }
func (s *pingFindNodeDriverSocket) Receive(buffer []byte) (int, netip.AddrPort, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receiveCalls++
	s.events = append(s.events, "receive")
	if s.served {
		return 0, netip.AddrPort{}, context.Canceled
	}
	s.served = true
	copy(buffer, s.wire)
	s.cancel()
	return len(s.wire), s.source, nil
}

func (s *pingFindNodeDriverSocket) Send(destination netip.AddrPort, wire []byte) error {
	s.mu.Lock()
	s.events = append(s.events, "send")
	s.sends = append(s.sends, pingFindNodeDriverSent{
		destination: destination,
		wire:        append([]byte(nil), wire...),
	})
	err := s.sendErr
	s.mu.Unlock()
	select {
	case s.sendObserved <- struct{}{}:
	default:
	}
	return err
}

type pingFindNodeDriverResponder struct {
	mu       *sync.Mutex
	events   *[]string
	response dht.Return
	err      error
}

func (r pingFindNodeDriverResponder) Respond(context.Context, dht.RecvMsg) (dht.Return, error) {
	r.mu.Lock()
	*r.events = append(*r.events, "respond")
	r.mu.Unlock()
	return r.response, r.err
}

func TestGenerateDHTPingFindNodeDriverParity(t *testing.T) {
	origin := pingFindNodeDriverID(0x90)
	node := dht.NodeInfo{
		ID:   pingFindNodeDriverID(1),
		Addr: dht.NewNodeAddrFromAddrPort(netip.MustParseAddrPort("192.0.2.9:0")),
	}
	remote := netip.MustParseAddrPort("192.0.2.1:6881")
	mapped := netip.MustParseAddrPort("[::ffff:192.0.2.2]:6882")
	scoped := netip.MustParseAddrPort("[fe80::3%7]:6883")
	args := func(target protocol.ID) *dht.MsgArgs {
		return &dht.MsgArgs{ID: pingFindNodeDriverID(2), Target: target}
	}
	message := func(tid, kind, query string, arguments *dht.MsgArgs) []byte {
		return mustPingFindNodeDriverBencode(t, dht.Msg{T: tid, Y: kind, Q: query, A: arguments})
	}
	errorResponse := func(tid string) []byte {
		return mustPingFindNodeDriverBencode(t, dht.Msg{
			T: tid, Y: dht.YError,
			E: &dht.Error{Code: dht.ErrorCodeServerError, Msg: "remote"},
		})
	}
	response := func(tid string) []byte {
		return mustPingFindNodeDriverBencode(t, dht.Msg{
			T: tid, Y: dht.YResponse, R: &dht.Return{ID: origin},
		})
	}
	mixed := dht.Msg{
		T: "M1", Y: dht.YQuery, Q: dht.QPing, A: args(protocol.ID{}),
		R:        &dht.Return{ID: pingFindNodeDriverID(5)},
		E:        &dht.Error{Code: 999, Msg: "request-only"},
		ReadOnly: true, ClientID: "client",
	}
	scenarios := []pingFindNodeDriverScenario{
		{id: "zero_length", rustOutcome: "zero", source: remote, origin: origin},
		{id: "malformed", rustOutcome: "decode_rejected", wire: []byte("d1:t2:X1"), source: remote, origin: origin},
		{id: "missing_type_ignored", rustOutcome: "ignored", wire: message("I1", "", dht.QPing, args(protocol.ID{})), source: remote, origin: origin},
		{id: "unknown_type_ignored", rustOutcome: "ignored", wire: message("I2", "x", dht.QPing, args(protocol.ID{})), source: remote, origin: origin},
		{id: "response_delivered_no_send", rustOutcome: "response_delivered", wire: response("R1"), source: remote, origin: origin, pendingTID: "R1", expectedSource: remote},
		{id: "error_delivered_no_send", rustOutcome: "error_delivered", wire: errorResponse("E1"), source: remote, origin: origin, pendingTID: "E1", expectedSource: remote},
		{id: "ping_success_empty_tid", rustOutcome: "reply_sent", wire: message("", dht.YQuery, dht.QPing, args(protocol.ID{})), source: remote, origin: origin, response: dht.Return{ID: origin}},
		{id: "ping_missing_arguments_one_byte_tid", rustOutcome: "reply_sent", wire: message("P", dht.YQuery, dht.QPing, nil), source: scoped, origin: origin, responseErr: dht.Error{Code: dht.ErrorCodeProtocolError, Msg: "missing arguments"}},
		{id: "find_node_three_byte_tid_mapped_source", rustOutcome: "reply_sent", wire: message("FN1", dht.YQuery, dht.QFindNode, args(node.ID)), source: mapped, origin: origin, nodes: []dht.NodeInfo{node}, response: dht.Return{ID: origin, Nodes: dht.CompactIPv4NodeInfo{node}}},
		{id: "mixed_request_fields_cleared", rustOutcome: "reply_sent", wire: mustPingFindNodeDriverBencode(t, mixed), source: remote, origin: origin, response: dht.Return{ID: origin}},
		{id: "unowned_query_is_partial_no_reply", rustOutcome: "unowned_query", wire: message("U1", dht.YQuery, dht.QGetPeers, args(protocol.ID{})), source: remote, origin: origin, responseErr: dht.Error{Code: dht.ErrorCodeMethodUnknown, Msg: "method Unknown"}},
		{id: "send_failure_is_typed", rustOutcome: "send_error", wire: message("S1", dht.YQuery, dht.QPing, args(protocol.ID{})), source: remote, origin: origin, response: dht.Return{ID: origin}, sendFails: true},
	}

	fixtures := make([]pingFindNodeDriverFixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixtures = append(fixtures, runPingFindNodeDriverScenario(t, scenario))
	}
	reconcilePingFindNodeDriverFixtures(t, fixtures)
}

func runPingFindNodeDriverScenario(t *testing.T, scenario pingFindNodeDriverScenario) pingFindNodeDriverFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	sentinel := errors.New("driver oracle send sentinel")
	socket := &pingFindNodeDriverSocket{
		wire: scenario.wire, source: scenario.source, cancel: cancel,
		sendObserved: make(chan struct{}, 1),
	}
	logCore, observedLogs := observer.New(zap.DebugLevel)
	if scenario.sendFails {
		socket.sendErr = sentinel
	}
	server := &server{
		socket: socket, queries: make(map[string]pendingQuery),
		responder: pingFindNodeDriverResponder{
			mu: &socket.mu, events: &socket.events,
			response: scenario.response, err: scenario.responseErr,
		},
		responderTimeout: time.Minute,
		logger:           zap.New(logCore).Sugar(),
	}
	var pending chan dht.RecvMsg
	if scenario.pendingTID != "" {
		pending = make(chan dht.RecvMsg, 1)
		server.queries[scenario.pendingTID] = pendingQuery{ch: pending, addr: scenario.expectedSource}
	}
	server.read(ctx)

	expected := pingFindNodeDriverExpected{
		RustOutcome: scenario.rustOutcome,
		RustEvents:  []string{"receive"},
	}
	var decoded dht.Msg
	decodeErr := bencode.Unmarshal(scenario.wire, &decoded)
	switch {
	case len(scenario.wire) == 0:
		expected.GoOutcome = "zero"
	case decodeErr != nil:
		expected.GoOutcome = "decode_rejected"
	case decoded.Y == dht.YResponse || decoded.Y == dht.YError:
		select {
		case <-pending:
		case <-time.After(time.Second):
			t.Fatalf("%s: response handler did not complete", scenario.id)
		}
		if decoded.Y == dht.YError {
			expected.GoOutcome = "error_delivered"
		} else {
			expected.GoOutcome = "response_delivered"
		}
	case decoded.Y != dht.YQuery:
		expected.GoOutcome = "ignored"
	default:
		select {
		case <-socket.sendObserved:
		case <-time.After(time.Second):
			t.Fatalf("%s: query send did not complete", scenario.id)
		}
		expected.GoOutcome = "reply_sent"
		expected.RustEvents = []string{"receive", "send"}
		if scenario.id == "unowned_query_is_partial_no_reply" {
			expected.IntentionalPartial = true
			expected.RustEvents = []string{"receive"}
		}
		if scenario.sendFails {
			expected.GoOutcome = "send_error_swallowed"
		}
	}
	if scenario.sendFails {
		waitForPingFindNodeDriverSendFailureLog(t, observedLogs, sentinel)
		expected.SendFailureLogged = true
	} else if count := observedLogs.FilterMessage("could not send response").Len(); count != 0 {
		t.Fatalf("%s: unexpected send-failure logs: %d", scenario.id, count)
	}

	socket.mu.Lock()
	expected.Events = append([]string(nil), socket.events...)
	expected.ReceiveCalls = socket.receiveCalls
	expected.SendCalls = len(socket.sends)
	if len(socket.sends) == 1 {
		sent := socket.sends[0]
		destination := pingFindNodeDriverProjectAddr(sent.destination)
		expected.Destination = &destination
		expected.WireHex = hex.EncodeToString(sent.wire)
	}
	socket.mu.Unlock()
	server.mutex.Lock()
	_, expected.PendingAfter = server.queries[scenario.pendingTID]
	server.mutex.Unlock()
	if expected.ReceiveCalls != 1 {
		t.Fatalf("%s: expected exactly one receive, got %d", scenario.id, expected.ReceiveCalls)
	}
	if expected.SendCalls > 1 {
		t.Fatalf("%s: sent more than once", scenario.id)
	}
	input := pingFindNodeDriverInput{
		WireHex:       hex.EncodeToString(scenario.wire),
		Source:        pingFindNodeDriverProjectAddr(scenario.source),
		Origin:        scenario.origin.String(),
		SendFails:     scenario.sendFails,
		PendingTIDHex: hex.EncodeToString([]byte(scenario.pendingTID)),
	}
	for _, node := range scenario.nodes {
		input.Nodes = append(input.Nodes, pingFindNodeDriverNode{
			ID: node.ID.String(), Addr: pingFindNodeDriverProjectAddr(node.Addr.ToAddrPort()),
		})
	}
	if scenario.pendingTID != "" {
		addr := pingFindNodeDriverProjectAddr(scenario.expectedSource)
		input.ExpectedSource = &addr
	}
	return pingFindNodeDriverFixture{
		ID: scenario.id, Subsystem: "dht_ping_find_node_driver",
		Input: input, Expected: expected,
	}
}

func waitForPingFindNodeDriverSendFailureLog(
	t *testing.T,
	logs *observer.ObservedLogs,
	sentinel error,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		entries := logs.FilterMessage("could not send response").All()
		if len(entries) == 1 {
			if entries[0].Level != zap.DebugLevel {
				t.Fatalf("send failure log level = %s, want debug", entries[0].Level)
			}
			var exactSentinel bool
			for _, field := range entries[0].Context {
				if field.Key == "retErr" && field.Interface == sentinel {
					exactSentinel = true
				}
			}
			if !exactSentinel {
				t.Fatalf("send failure log did not retain the exact transport sentinel: %#v", entries[0].Context)
			}
			return
		}
		if len(entries) > 1 {
			t.Fatalf("send failure produced %d completion logs, want exactly one", len(entries))
		}
		if time.Now().After(deadline) {
			t.Fatal("handleQuery did not log the swallowed send failure")
		}
		runtime.Gosched()
	}
}

func mustPingFindNodeDriverBencode(t *testing.T, message dht.Msg) []byte {
	t.Helper()
	wire, err := bencode.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func pingFindNodeDriverID(last byte) protocol.ID {
	var id protocol.ID
	id[19] = last
	return id
}

func pingFindNodeDriverProjectAddr(addr netip.AddrPort) pingFindNodeDriverAddr {
	scope := uint32(0)
	if addr.Addr().Zone() != "" {
		parsed, err := strconv.ParseUint(addr.Addr().Zone(), 10, 32)
		if err != nil {
			panic(err)
		}
		scope = uint32(parsed)
	}
	return pingFindNodeDriverAddr{
		IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: scope,
	}
}

func reconcilePingFindNodeDriverFixtures(t *testing.T, fixtures []pingFindNodeDriverFixture) {
	t.Helper()
	var encoded bytes.Buffer
	for _, fixture := range fixtures {
		line, err := json.Marshal(fixture)
		if err != nil {
			t.Fatal(err)
		}
		encoded.Write(line)
		encoded.WriteByte('\n')
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve generator source")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../testdata/parity/dht/ping_find_node_driver.jsonl"))
	if *updateDHTPingFindNodeDriverParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-ping-find-node-driver-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("ping/find-node driver fixture is stale; rerun with -update-dht-ping-find-node-driver-parity")
	}
}
