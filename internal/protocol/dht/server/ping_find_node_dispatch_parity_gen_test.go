package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"go.uber.org/zap"
)

var updateDHTPingFindNodeDispatchParity = flag.Bool(
	"update-dht-ping-find-node-dispatch-parity",
	false,
	"rewrite the Rust DHT ping/find-node dispatch parity fixture",
)

type pingFindNodeDispatchFixture struct {
	ID        string                       `json:"id"`
	Subsystem string                       `json:"subsystem"`
	Input     pingFindNodeDispatchInput    `json:"input"`
	Expected  pingFindNodeDispatchExpected `json:"expected"`
}

type pingFindNodeDispatchInput struct {
	Source  pingFindNodeDispatchAddr    `json:"source"`
	Request pingFindNodeDispatchRequest `json:"request"`
	Script  pingFindNodeDispatchScript  `json:"script"`
}

type pingFindNodeDispatchAddr struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope,omitempty"`
}

type pingFindNodeDispatchRequest struct {
	TIDHex      string `json:"tidHex"`
	TypeHex     string `json:"typeHex"`
	MethodHex   string `json:"methodHex"`
	ArgsPresent bool   `json:"argsPresent"`
	MixedFields bool   `json:"mixedFields,omitempty"`
}

type pingFindNodeDispatchScript struct {
	Kind        string                    `json:"kind"`
	ResponseID  string                    `json:"responseId,omitempty"`
	Node        *pingFindNodeDispatchNode `json:"node,omitempty"`
	ErrorCode   int                       `json:"errorCode,omitempty"`
	ErrorString string                    `json:"errorString,omitempty"`
}

type pingFindNodeDispatchNode struct {
	ID   string                   `json:"id"`
	Addr pingFindNodeDispatchAddr `json:"addr"`
}

type pingFindNodeDispatchExpected struct {
	Destination       pingFindNodeDispatchAddr `json:"destination"`
	WireHex           string                   `json:"wireHex,omitempty"`
	GoPanicked        bool                     `json:"goPanicked,omitempty"`
	Generic202WireHex string                   `json:"generic202WireHex,omitempty"`
}

type pingFindNodeScriptedResponder struct {
	response dht.Return
	err      error
}

func (r pingFindNodeScriptedResponder) Respond(context.Context, dht.RecvMsg) (dht.Return, error) {
	return r.response, r.err
}

type pingFindNodeCaptureSocket struct {
	destination netip.AddrPort
	wire        []byte
}

func (*pingFindNodeCaptureSocket) Open(netip.AddrPort) error { return nil }
func (*pingFindNodeCaptureSocket) Close() error              { return nil }
func (s *pingFindNodeCaptureSocket) Send(destination netip.AddrPort, wire []byte) error {
	s.destination = destination
	s.wire = append([]byte(nil), wire...)
	return nil
}
func (*pingFindNodeCaptureSocket) Receive([]byte) (int, netip.AddrPort, error) {
	return 0, netip.AddrPort{}, errors.New("receive is outside the dispatch oracle")
}

func TestGenerateDHTPingFindNodeDispatchParity(t *testing.T) {
	localID := dispatchID(0x90)
	nodeID := dispatchID(1)
	scenarios := []struct {
		id    string
		input pingFindNodeDispatchInput
	}{
		{"success_ping_two_byte_tid", dispatchInput("192.0.2.1", 6881, 0, "0102", "71", "70696e67", true, false, pingFindNodeDispatchScript{Kind: "success", ResponseID: localID})},
		{"protocol_error_empty_tid", dispatchInput("192.0.2.2", 0, 0, "", "71", "70696e67", false, false, pingFindNodeDispatchScript{Kind: "protocol", ErrorCode: 203, ErrorString: "missing arguments"})},
		{"wrapped_protocol_one_byte_tid", dispatchInput("192.0.2.3", 1, 0, "ff", "71", "66696e645f6e6f6465", true, false, pingFindNodeDispatchScript{Kind: "wrapped", ErrorCode: 207, ErrorString: "wrapped protocol"})},
		{"wrapped_pointer_is_generic", dispatchInput("192.0.2.30", 1, 0, "fe", "71", "70696e67", true, false, pingFindNodeDispatchScript{Kind: "wrappedPointer", ErrorCode: 207, ErrorString: "wrapped pointer"})},
		{"generic_error_binary_tid", dispatchInput("192.0.2.4", 2, 0, "00ff", "71", "70696e67", true, false, pingFindNodeDispatchScript{Kind: "generic"})},
		{"generic_error_reference_tid", dispatchInput("192.0.2.40", 2, 0, "0102", "71", "70696e67", true, false, pingFindNodeDispatchScript{Kind: "generic"})},
		{"success_node_three_byte_tid_mapped_source", dispatchInput("::ffff:192.0.2.5", 3, 0, "000102", "71", "66696e645f6e6f6465", true, false, pingFindNodeDispatchScript{Kind: "success", ResponseID: localID, Node: &pingFindNodeDispatchNode{ID: nodeID, Addr: pingFindNodeDispatchAddr{IP: "192.0.2.9", Port: 0}}})},
		{"mixed_request_fields_are_cleared", dispatchInput("2001:db8::6", 4, 0, "aa55", "65", "70696e67", true, true, pingFindNodeDispatchScript{Kind: "success", ResponseID: localID})},
		{"scoped_source_is_exact", dispatchInput("fe80::7", 5, 7, "31", "78", "70696e67", false, false, pingFindNodeDispatchScript{Kind: "protocol", ErrorCode: 203, ErrorString: "missing arguments"})},
		{"native_ipv6_response_panics", dispatchInput("2001:db8::8", 6, 0, "4e31", "71", "66696e645f6e6f6465", true, false, pingFindNodeDispatchScript{Kind: "native", ResponseID: localID, Node: &pingFindNodeDispatchNode{ID: nodeID, Addr: pingFindNodeDispatchAddr{IP: "2001:db8::9", Port: 6881}}})},
	}

	fixtures := make([]pingFindNodeDispatchFixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixtures = append(fixtures, runPingFindNodeDispatchScenario(t, scenario.id, scenario.input))
	}
	reconcilePingFindNodeDispatchFixtures(t, fixtures)
}

func runPingFindNodeDispatchScenario(t *testing.T, id string, input pingFindNodeDispatchInput) pingFindNodeDispatchFixture {
	t.Helper()
	request := input.Request.message(t)
	response, responseErr := input.Script.result(t)
	destination, wire, panicked := invokeRealHandleQuery(input.Source.addrPort(), request, response, responseErr)
	expected := pingFindNodeDispatchExpected{Destination: input.Source, GoPanicked: panicked}
	if !panicked {
		if destination != input.Source.addrPort() {
			t.Fatalf("%s: destination changed: %s", id, destination)
		}
		expected.WireHex = hex.EncodeToString(wire)
	}
	if input.Script.Kind == "native" {
		if !panicked {
			t.Fatalf("%s: native IPv6 compact-v4 response did not panic", id)
		}
		_, genericWire, genericPanicked := invokeRealHandleQuery(
			input.Source.addrPort(),
			request,
			dht.Return{},
			errors.New("local native IPv6 failure"),
		)
		if genericPanicked {
			t.Fatalf("%s: generic server error panicked", id)
		}
		expected.Generic202WireHex = hex.EncodeToString(genericWire)
	}
	return pingFindNodeDispatchFixture{ID: id, Subsystem: "dht_ping_find_node_dispatch", Input: input, Expected: expected}
}

func invokeRealHandleQuery(source netip.AddrPort, request dht.Msg, response dht.Return, responseErr error) (destination netip.AddrPort, wire []byte, panicked bool) {
	socket := &pingFindNodeCaptureSocket{}
	server := &server{
		socket:           socket,
		responder:        pingFindNodeScriptedResponder{response: response, err: responseErr},
		responderTimeout: time.Second,
		logger:           zap.NewNop().Sugar(),
	}
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	server.handleQuery(context.Background(), dht.RecvMsg{Msg: request, From: source})
	return socket.destination, socket.wire, false
}

func (value pingFindNodeDispatchRequest) message(t *testing.T) dht.Msg {
	t.Helper()
	message := dht.Msg{
		T: string(mustDispatchHex(t, value.TIDHex)),
		Y: string(mustDispatchHex(t, value.TypeHex)),
		Q: string(mustDispatchHex(t, value.MethodHex)),
	}
	if value.ArgsPresent {
		message.A = &dht.MsgArgs{ID: protocol.ID{}}
	}
	if value.MixedFields {
		message.R = &dht.Return{ID: protocol.ID{1}}
		message.E = &dht.Error{Code: 999, Msg: "request-only"}
		message.IP = dht.NewNodeAddrFromAddrPort(netip.MustParseAddrPort("198.51.100.1:9"))
		message.ReadOnly = true
		message.ClientID = "client"
	}
	return message
}

func (value pingFindNodeDispatchScript) result(t *testing.T) (dht.Return, error) {
	t.Helper()
	response := dht.Return{}
	if value.ResponseID != "" {
		response.ID = protocol.MustParseID(value.ResponseID)
	}
	if value.Node != nil {
		response.Nodes = dht.CompactIPv4NodeInfo{{
			ID:   protocol.MustParseID(value.Node.ID),
			Addr: dht.NewNodeAddrFromAddrPort(value.Node.Addr.addrPort()),
		}}
	}
	switch value.Kind {
	case "success", "native":
		return response, nil
	case "protocol":
		return response, dht.Error{Code: value.ErrorCode, Msg: value.ErrorString}
	case "wrapped":
		return response, fmt.Errorf("outer: %w", dht.Error{Code: value.ErrorCode, Msg: value.ErrorString})
	case "wrappedPointer":
		return response, fmt.Errorf("outer: %w", &dht.Error{Code: value.ErrorCode, Msg: value.ErrorString})
	case "generic":
		return response, errors.New("private failure")
	default:
		t.Fatalf("unknown script kind %q", value.Kind)
		return dht.Return{}, nil
	}
}

func (value pingFindNodeDispatchAddr) addrPort() netip.AddrPort {
	ip := netip.MustParseAddr(value.IP)
	if value.Scope != 0 {
		ip = ip.WithZone(fmt.Sprint(value.Scope))
	}
	return netip.AddrPortFrom(ip, value.Port)
}

func dispatchInput(ip string, port uint16, scope uint32, tid, kind, method string, args, mixed bool, script pingFindNodeDispatchScript) pingFindNodeDispatchInput {
	return pingFindNodeDispatchInput{
		Source:  pingFindNodeDispatchAddr{IP: ip, Port: port, Scope: scope},
		Request: pingFindNodeDispatchRequest{TIDHex: tid, TypeHex: kind, MethodHex: method, ArgsPresent: args, MixedFields: mixed},
		Script:  script,
	}
}

func dispatchID(last byte) string {
	var id protocol.ID
	id[19] = last
	return id.String()
}

func mustDispatchHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func reconcilePingFindNodeDispatchFixtures(t *testing.T, fixtures []pingFindNodeDispatchFixture) {
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
	path := filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../testdata/parity/dht/ping_find_node_dispatch.jsonl"))
	if *updateDHTPingFindNodeDispatchParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-ping-find-node-dispatch-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("ping/find-node dispatch fixture is stale; rerun with -update-dht-ping-find-node-dispatch-parity")
	}
}
