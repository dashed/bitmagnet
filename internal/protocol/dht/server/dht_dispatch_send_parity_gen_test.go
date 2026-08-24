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
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var updateDHTDispatchSendParity = flag.Bool(
	"update-dht-dispatch-send-parity",
	false,
	"rewrite the Rust DHT full dispatch/send parity fixture",
)

type dhtDispatchSendFixture struct {
	ID        string                  `json:"id"`
	Subsystem string                  `json:"subsystem"`
	Runtime   dhtDispatchSendRuntime  `json:"runtime"`
	Input     dhtDispatchSendInput    `json:"input"`
	Expected  dhtDispatchSendExpected `json:"expected"`
}

type dhtDispatchSendRuntime struct {
	IntBits int `json:"intBits"`
}

type dhtDispatchSendInput struct {
	Source    dhtDispatchSendAddr           `json:"source"`
	Request   dhtDispatchSendRequest        `json:"request"`
	Context   string                        `json:"context"`
	Responder dhtDispatchSendResponderInput `json:"responder"`
	Socket    dhtDispatchSendSocketInput    `json:"socket"`
	State     dhtDispatchSendMutationState  `json:"state"`
}

type dhtDispatchSendAddr struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope,omitempty"`
}

type dhtDispatchSendRequest struct {
	TIDHex      string `json:"tidHex"`
	TypeHex     string `json:"typeHex"`
	MethodHex   string `json:"methodHex"`
	ArgsPresent bool   `json:"argsPresent"`
	MixedFields bool   `json:"mixedFields,omitempty"`
}

type dhtDispatchSendResponderInput struct {
	Kind          string                `json:"kind"`
	Return        dhtDispatchSendReturn `json:"return"`
	ErrorCode     int                   `json:"errorCode,omitempty"`
	ErrorHex      string                `json:"errorHex,omitempty"`
	Mutation      string                `json:"mutation,omitempty"`
	UnsupportedV  bool                  `json:"unsupportedV,omitempty"`
	ReturnsCtxErr bool                  `json:"returnsContextError,omitempty"`
	Panics        bool                  `json:"panics,omitempty"`
}

type dhtDispatchSendSocketInput struct {
	Kind string `json:"kind"`
}

type dhtDispatchSendExpected struct {
	ResponderCalls         int                           `json:"responderCalls"`
	ResponderInputExact    bool                          `json:"responderInputExact"`
	Context                dhtDispatchSendContext        `json:"context"`
	Classification         string                        `json:"classification"`
	Destination            *dhtDispatchSendAddr          `json:"destination,omitempty"`
	WireHex                string                        `json:"wireHex,omitempty"`
	Envelope               *dhtDispatchSendEnvelope      `json:"envelope,omitempty"`
	Events                 []string                      `json:"events"`
	SendCalls              int                           `json:"sendCalls"`
	Logs                   []dhtDispatchSendLog          `json:"logs"`
	State                  dhtDispatchSendExpectedState  `json:"state"`
	PartialReturnDiscarded bool                          `json:"partialReturnDiscarded,omitempty"`
	SendFailureSwallowed   bool                          `json:"sendFailureSwallowed,omitempty"`
	ReturnedError          *dhtDispatchSendReturnedError `json:"returnedError,omitempty"`
	Terminal               string                        `json:"terminal"`
	PanicText              string                        `json:"panicText,omitempty"`
	PanicIdentityExact     bool                          `json:"panicIdentityExact,omitempty"`
}

type dhtDispatchSendContext struct {
	DeadlinePresent bool   `json:"deadlinePresent"`
	ErrAtRespond    string `json:"errAtRespond"`
	ErrAfter        string `json:"errAfter"`
}

type dhtDispatchSendEnvelope struct {
	TIDHex        string                      `json:"tidHex"`
	TypeHex       string                      `json:"typeHex"`
	Presence      dhtDispatchSendWirePresence `json:"presence"`
	Return        *dhtDispatchSendReturn      `json:"return,omitempty"`
	Error         *dhtDispatchSendError       `json:"error,omitempty"`
	Canonical     bool                        `json:"canonical"`
	TIDEchoed     bool                        `json:"tidEchoed"`
	FieldsCleared bool                        `json:"requestFieldsCleared"`
}

type dhtDispatchSendWirePresence struct {
	Query     bool `json:"q"`
	Arguments bool `json:"a"`
	Return    bool `json:"r"`
	Error     bool `json:"e"`
	IP        bool `json:"ip"`
	ReadOnly  bool `json:"ro"`
	ClientID  bool `json:"v"`
	ID        bool `json:"id"`
	Nodes     bool `json:"nodes"`
	Nodes6    bool `json:"nodes6"`
	Values    bool `json:"values"`
	Token     bool `json:"token"`
	Samples   bool `json:"samples"`
	Num       bool `json:"num"`
	Interval  bool `json:"interval"`
	SeedersBF bool `json:"BFsd"`
	PeersBF   bool `json:"BFpe"`
}

type dhtDispatchSendReturn struct {
	ID              string                    `json:"id"`
	NodesPresent    bool                      `json:"nodesPresent"`
	Nodes           []dhtDispatchSendNode     `json:"nodes"`
	Nodes6Present   bool                      `json:"nodes6Present"`
	Nodes6          []dhtDispatchSendNode     `json:"nodes6"`
	ValuesPresent   bool                      `json:"valuesPresent"`
	Values          []dhtDispatchSendPeerAddr `json:"values"`
	TokenPresent    bool                      `json:"tokenPresent"`
	TokenHex        string                    `json:"tokenHex"`
	SamplesPresent  bool                      `json:"samplesPresent"`
	Samples         []string                  `json:"samples"`
	NumPresent      bool                      `json:"numPresent"`
	Num             int64                     `json:"num"`
	IntervalPresent bool                      `json:"intervalPresent"`
	Interval        int64                     `json:"interval"`
}

type dhtDispatchSendNode struct {
	ID   string                  `json:"id"`
	Addr dhtDispatchSendPeerAddr `json:"addr"`
}

type dhtDispatchSendPeerAddr struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

type dhtDispatchSendError struct {
	Code       int    `json:"code"`
	MessageHex string `json:"messageHex"`
}

type dhtDispatchSendLog struct {
	Level          string `json:"level"`
	Message        string `json:"message"`
	RetErrKey      bool   `json:"retErrKey"`
	RetErrType     string `json:"retErrType"`
	RetErrText     string `json:"retErrText"`
	RetErrIdentity bool   `json:"retErrIdentityExact"`
}

type dhtDispatchSendReturnedError struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type dhtDispatchSendMutationState struct {
	Mutations []string `json:"mutations"`
}

type dhtDispatchSendExpectedState struct {
	Before []string `json:"before"`
	AtSend []string `json:"atSend"`
	After  []string `json:"after"`
}

type dhtDispatchSendScenario struct {
	id                   string
	source               netip.AddrPort
	request              dht.Msg
	contextKind          string
	responderTimeout     time.Duration
	response             dht.Return
	responseErr          error
	responseErrKind      string
	returnsContextError  bool
	responderPanic       error
	mutation             string
	unsupportedV         bool
	directSend           bool
	directMessage        dht.Msg
	socketErr            error
	socketPanic          error
	wantClassification   string
	wantContextAtRespond string
	wantContextAfter     string
	wantEvents           []string
	wantSendCalls        int
	wantServerLogs       int
	wantSendLogs         int
	wantTerminal         string
	wantPanicText        string
	wantPanicIdentity    bool
}

type dhtDispatchSendScriptedResponder struct {
	scenario         *dhtDispatchSendScenario
	events           *[]string
	state            *dhtDispatchSendMutationState
	calls            int
	observed         dht.RecvMsg
	context          context.Context
	contextAtRespond string
	deadlinePresent  bool
	returnedErr      error
}

func (r *dhtDispatchSendScriptedResponder) Respond(ctx context.Context, msg dht.RecvMsg) (dht.Return, error) {
	r.calls++
	r.observed = msg
	r.context = ctx
	r.contextAtRespond = dhtDispatchSendContextError(ctx.Err())
	_, r.deadlinePresent = ctx.Deadline()
	*r.events = append(*r.events, "respond")
	if r.scenario.mutation != "" {
		*r.events = append(*r.events, "mutate")
		r.state.Mutations = append(r.state.Mutations, r.scenario.mutation)
	}
	if r.scenario.responderPanic != nil {
		panic(r.scenario.responderPanic)
	}
	if r.scenario.returnsContextError {
		r.returnedErr = ctx.Err()
		return r.scenario.response, r.returnedErr
	}
	r.returnedErr = r.scenario.responseErr
	return r.scenario.response, r.returnedErr
}

type dhtDispatchSendCaptureSocket struct {
	events       *[]string
	state        *dhtDispatchSendMutationState
	err          error
	panicValue   error
	destinations []netip.AddrPort
	wires        [][]byte
	stateAtSend  []string
}

func (*dhtDispatchSendCaptureSocket) Open(netip.AddrPort) error { return nil }
func (*dhtDispatchSendCaptureSocket) Close() error              { return nil }
func (*dhtDispatchSendCaptureSocket) Receive([]byte) (int, netip.AddrPort, error) {
	return 0, netip.AddrPort{}, errors.New("receive is outside the dispatch/send oracle")
}
func (s *dhtDispatchSendCaptureSocket) Send(destination netip.AddrPort, wire []byte) error {
	*s.events = append(*s.events, "send")
	s.destinations = append(s.destinations, destination)
	s.wires = append(s.wires, append([]byte(nil), wire...))
	s.stateAtSend = append([]string(nil), s.state.Mutations...)
	if s.panicValue != nil {
		panic(s.panicValue)
	}
	return s.err
}

func TestGenerateDHTDispatchSendParity(t *testing.T) {
	if strconv.IntSize != 64 {
		t.Fatalf("DHT dispatch/send parity requires 64-bit Go int, got %d", strconv.IntSize)
	}

	scenarios := dhtDispatchSendScenarios()
	fixtures := make([]dhtDispatchSendFixture, 0, len(scenarios))
	for index := range scenarios {
		fixtures = append(fixtures, runDHTDispatchSendScenario(t, &scenarios[index]))
	}
	assertDHTDispatchSendIndependentGoldens(t, fixtures)
	reconcileDHTDispatchSendFixtures(t, fixtures)
}

func dhtDispatchSendScenarios() []dhtDispatchSendScenario {
	origin := dhtDispatchSendID(0x90)
	requester := dhtDispatchSendID(0x02)
	args := &dht.MsgArgs{ID: requester, Target: dhtDispatchSendID(0x03), InfoHash: dhtDispatchSendID(0x04)}
	node4a := dhtDispatchSendNodeInfo(0x11, "192.0.2.11:11")
	node4b := dhtDispatchSendNodeInfo(0x12, "198.51.100.12:65535")
	value4 := dht.NewNodeAddrFromAddrPort(netip.MustParseAddrPort("203.0.113.21:0"))
	value6 := dht.NewNodeAddrFromAddrPort(netip.MustParseAddrPort("[2001:db8::22]:6881"))
	token := string([]byte{0x00, 0xff, 0x54})
	emptyToken := ""
	populatedSamples := dht.CompactInfohashes{
		dhtDispatchSendID(0x31), dhtDispatchSendID(0x31), dhtDispatchSendID(0x32),
	}
	emptySamples := make(dht.CompactInfohashes, 0)
	minInt64 := int64(-1 << 63)
	maxInt64 := int64(1<<63 - 1)
	zero := int64(0)
	partial := dht.Return{
		ID:     dhtDispatchSendID(0xee),
		Nodes:  dht.CompactIPv4NodeInfo{node4a},
		Values: []dht.NodeAddr{value4},
		Token:  &token,
	}

	request := func(tid []byte, y, method string, mixed bool) dht.Msg {
		msg := dht.Msg{T: string(tid), Y: y, Q: method, A: args}
		if mixed {
			msg.R = &dht.Return{ID: dhtDispatchSendID(0xaa)}
			msg.E = &dht.Error{Code: 999, Msg: "request-only"}
			msg.IP = dht.NewNodeAddrFromAddrPort(netip.MustParseAddrPort("198.51.100.200:9"))
			msg.ReadOnly = true
			msg.ClientID = string([]byte{0xff, 0x00})
		}
		return msg
	}
	success := func(id string, source netip.AddrPort, msg dht.Msg, ret dht.Return) dhtDispatchSendScenario {
		return dhtDispatchSendScenario{
			id: id, source: source, request: msg, contextKind: "active",
			responderTimeout: time.Minute, response: ret, responseErrKind: "none",
			wantClassification: "success", wantContextAtRespond: "none", wantContextAfter: "canceled",
			wantEvents: []string{"respond", "send"}, wantSendCalls: 1, wantTerminal: "returned",
		}
	}
	errorScenario := func(
		id string,
		source netip.AddrPort,
		msg dht.Msg,
		err error,
		kind string,
		classification string,
		serverLogs int,
	) dhtDispatchSendScenario {
		return dhtDispatchSendScenario{
			id: id, source: source, request: msg, contextKind: "active", responderTimeout: time.Minute,
			response: partial, responseErr: err, responseErrKind: kind,
			wantClassification: classification, wantContextAtRespond: "none", wantContextAfter: "canceled",
			wantEvents: []string{"respond", "send"}, wantSendCalls: 1,
			wantServerLogs: serverLogs, wantTerminal: "returned",
		}
	}

	remote := netip.MustParseAddrPort("192.0.2.1:6881")
	mapped := netip.MustParseAddrPort("[::ffff:192.0.2.2]:6882")
	scoped := netip.MustParseAddrPort("[fe80::3%7]:6883")
	native6 := netip.MustParseAddrPort("[2001:db8::4]:6884")

	scenarios := []dhtDispatchSendScenario{
		success(
			"ping_success_empty_tid_mixed_request_y_ignored",
			remote,
			request(nil, dht.YError, dht.QPing, true),
			dht.Return{ID: origin},
		),
		success(
			"find_nodes_binary_tid_mapped_destination",
			mapped,
			request([]byte{0x00, 0xff, 0x01}, "x", dht.QFindNode, false),
			dht.Return{ID: origin, Nodes: dht.CompactIPv4NodeInfo{node4a, node4a, node4b}},
		),
		success(
			"get_peers_values_and_binary_token",
			remote,
			request([]byte("GV"), dht.YQuery, dht.QGetPeers, false),
			dht.Return{ID: origin, Values: []dht.NodeAddr{value4, value6, value4}, Token: &token},
		),
		success(
			"get_peers_values_present_empty_and_empty_token",
			remote,
			request([]byte("GE"), dht.YQuery, dht.QGetPeers, false),
			dht.Return{ID: origin, Values: make([]dht.NodeAddr, 0), Token: &emptyToken},
		),
		success(
			"get_peers_closest_nodes_and_token",
			native6,
			request([]byte("GN"), dht.YQuery, dht.QGetPeers, false),
			dht.Return{ID: origin, Nodes: dht.CompactIPv4NodeInfo{node4b, node4a}, Token: &token},
		),
		success(
			"find_nodes_present_empty",
			remote,
			request([]byte("FE"), dht.YQuery, dht.QFindNode, false),
			dht.Return{ID: origin, Nodes: make(dht.CompactIPv4NodeInfo, 0)},
		),
		success(
			"sample_populated_long_tid_signed_extremes",
			native6,
			request(bytes.Repeat([]byte{0xab}, 257), dht.YQuery, dht.QSampleInfohashes, false),
			dht.Return{
				ID: origin, Nodes: dht.CompactIPv4NodeInfo{node4a, node4b},
				Bep51Return: dht.Bep51Return{Samples: &populatedSamples, Num: &minInt64, Interval: &maxInt64},
			},
		),
		success(
			"sample_present_empty_and_zero_counts_scoped_destination",
			scoped,
			request([]byte("SE"), dht.YQuery, dht.QSampleInfohashes, false),
			dht.Return{ID: origin, Bep51Return: dht.Bep51Return{Samples: &emptySamples, Num: &zero, Interval: &zero}},
		),
	}

	announceSuccess := success(
		"announce_mutation_precedes_successful_send",
		remote,
		request([]byte("A1"), dht.YQuery, dht.QAnnouncePeer, false),
		dht.Return{ID: origin},
	)
	announceSuccess.mutation = "put_hash:0000000000000000000000000000000000000004@192.0.2.1:6881"
	announceSuccess.wantEvents = []string{"respond", "mutate", "send"}
	scenarios = append(scenarios, announceSuccess)

	direct203 := dht.Error{Code: dht.ErrorCodeProtocolError, Msg: "missing arguments"}
	direct204 := dht.Error{Code: dht.ErrorCodeMethodUnknown, Msg: "method Unknown"}
	scenarios = append(scenarios,
		errorScenario(
			"protocol_203_value_discards_partial_return",
			scoped,
			request(nil, "ignored", dht.QPing, true),
			direct203,
			"protocol_value",
			"protocol_203",
			0,
		),
		errorScenario(
			"protocol_203_wrapped_value",
			remote,
			request([]byte("W3"), dht.YQuery, dht.QPing, false),
			fmt.Errorf("outer 203: %w", direct203),
			"wrapped_protocol_value",
			"protocol_203",
			0,
		),
		errorScenario(
			"protocol_204_value",
			remote,
			request([]byte("U1"), dht.YQuery, "unknown", false),
			direct204,
			"protocol_value",
			"protocol_204",
			0,
		),
		errorScenario(
			"protocol_204_wrapped_value",
			remote,
			request([]byte("W4"), dht.YQuery, "unknown", false),
			fmt.Errorf("outer 204: %w", direct204),
			"wrapped_protocol_value",
			"protocol_204",
			0,
		),
	)

	pointerErr := &dht.Error{Code: 207, Msg: "pointer protocol"}
	wrappedPointerErr := fmt.Errorf("outer pointer: %w", &dht.Error{Code: 207, Msg: "wrapped pointer"})
	var typedNilPointer *dht.Error
	var typedNilError error = typedNilPointer
	genericErr := errors.New("dispatch generic sentinel")
	scenarios = append(scenarios,
		errorScenario(
			"direct_protocol_pointer_is_generic_202",
			remote,
			request([]byte("P1"), dht.YQuery, dht.QPing, false),
			pointerErr,
			"protocol_pointer",
			"generic_202",
			1,
		),
		errorScenario(
			"wrapped_protocol_pointer_is_generic_202",
			remote,
			request([]byte("P2"), dht.YQuery, dht.QPing, false),
			wrappedPointerErr,
			"wrapped_protocol_pointer",
			"generic_202",
			1,
		),
		errorScenario(
			"typed_nil_protocol_pointer_is_generic_202",
			remote,
			request([]byte("PN"), dht.YQuery, dht.QPing, false),
			typedNilError,
			"typed_nil_protocol_pointer",
			"generic_202",
			1,
		),
		errorScenario(
			"generic_error_binary_tid_discards_partial_return",
			remote,
			request([]byte{0x00, 0xff}, dht.YQuery, dht.QPing, false),
			genericErr,
			"generic",
			"generic_202",
			1,
		),
	)

	preCancelled := success(
		"pre_cancelled_context_success_still_sent",
		remote,
		request([]byte("C1"), dht.YQuery, dht.QPing, false),
		dht.Return{ID: origin},
	)
	preCancelled.contextKind = "cancelled"
	preCancelled.wantContextAtRespond = "canceled"
	preCancelled.wantContextAfter = "canceled"
	scenarios = append(scenarios, preCancelled)

	expiredSuccess := success(
		"already_expired_context_success_still_sent",
		remote,
		request([]byte("C2"), dht.YQuery, dht.QPing, false),
		dht.Return{ID: origin},
	)
	expiredSuccess.contextKind = "expired"
	expiredSuccess.responderTimeout = 0
	expiredSuccess.wantContextAtRespond = "deadline_exceeded"
	expiredSuccess.wantContextAfter = "deadline_exceeded"
	scenarios = append(scenarios, expiredSuccess)

	expiredError := errorScenario(
		"expired_context_error_becomes_generic_202",
		remote,
		request([]byte("C3"), dht.YQuery, dht.QPing, false),
		nil,
		"context_error",
		"generic_202",
		1,
	)
	expiredError.contextKind = "expired"
	expiredError.responderTimeout = 0
	expiredError.returnsContextError = true
	expiredError.wantContextAtRespond = "deadline_exceeded"
	expiredError.wantContextAfter = "deadline_exceeded"
	scenarios = append(scenarios, expiredError)

	transportSentinel := errors.New("dispatch transport sentinel")
	transportFailure := success(
		"transport_error_one_call_is_logged_and_swallowed",
		remote,
		request([]byte("S1"), dht.YQuery, dht.QPing, false),
		dht.Return{ID: origin},
	)
	transportFailure.socketErr = transportSentinel
	transportFailure.wantSendLogs = 1
	scenarios = append(scenarios, transportFailure)

	encodeFailure := success(
		"direct_send_returned_encode_error_zero_socket_calls",
		remote,
		request([]byte("E1"), dht.YQuery, dht.QPing, false),
		dht.Return{},
	)
	encodeFailure.unsupportedV = true
	encodeFailure.directSend = true
	encodeFailure.directMessage = request([]byte("E1"), dht.YQuery, dht.QPing, false)
	encodeFailure.directMessage.A.V = float64(1)
	encodeFailure.responseErrKind = "not_called_direct_send"
	encodeFailure.contextKind = "none_direct_send"
	encodeFailure.wantContextAtRespond = "none"
	encodeFailure.wantContextAfter = "none"
	encodeFailure.wantEvents = []string{}
	encodeFailure.wantSendCalls = 0
	encodeFailure.wantClassification = "direct_send_encode_error"
	encodeFailure.wantTerminal = "direct_send_returned"
	scenarios = append(scenarios, encodeFailure)

	compactPanic := success(
		"compact_ipv4_native_ipv6_panics_before_socket",
		remote,
		request([]byte("N1"), dht.YQuery, dht.QFindNode, false),
		dht.Return{ID: origin, Nodes: dht.CompactIPv4NodeInfo{
			dhtDispatchSendNodeInfo(0x41, "[2001:db8::41]:6881"),
		}},
	)
	compactPanic.wantEvents = []string{"respond"}
	compactPanic.wantSendCalls = 0
	compactPanic.wantTerminal = "panicked"
	compactPanic.wantPanicText = "marshalled 22 bytes, but expected 26"
	compactPanic.wantClassification = "success_encode_panic"
	scenarios = append(scenarios, compactPanic)

	responderPanicSentinel := errors.New("dispatch responder panic sentinel")
	responderPanic := success(
		"responder_panic_is_not_recovered",
		remote,
		request([]byte("R1"), dht.YQuery, dht.QPing, false),
		dht.Return{ID: origin},
	)
	responderPanic.responderPanic = responderPanicSentinel
	responderPanic.wantEvents = []string{"respond"}
	responderPanic.wantSendCalls = 0
	responderPanic.wantTerminal = "panicked"
	responderPanic.wantPanicText = responderPanicSentinel.Error()
	responderPanic.wantPanicIdentity = true
	responderPanic.wantClassification = "responder_panic"
	scenarios = append(scenarios, responderPanic)

	socketPanicSentinel := errors.New("dispatch socket panic sentinel")
	socketPanic := success(
		"socket_panic_after_one_call_is_not_recovered",
		remote,
		request([]byte("SP"), dht.YQuery, dht.QPing, false),
		dht.Return{ID: origin},
	)
	socketPanic.socketPanic = socketPanicSentinel
	socketPanic.wantTerminal = "panicked"
	socketPanic.wantPanicText = socketPanicSentinel.Error()
	socketPanic.wantPanicIdentity = true
	scenarios = append(scenarios, socketPanic)

	announceTransportFailure := success(
		"announce_mutation_precedes_failed_send_and_survives",
		remote,
		request([]byte("AF"), dht.YQuery, dht.QAnnouncePeer, false),
		dht.Return{ID: origin},
	)
	announceTransportFailure.mutation = "put_hash:0000000000000000000000000000000000000004@192.0.2.1:6881"
	announceTransportFailure.socketErr = transportSentinel
	announceTransportFailure.wantEvents = []string{"respond", "mutate", "send"}
	announceTransportFailure.wantSendLogs = 1
	scenarios = append(scenarios, announceTransportFailure)

	return scenarios
}

func runDHTDispatchSendScenario(t *testing.T, scenario *dhtDispatchSendScenario) dhtDispatchSendFixture {
	t.Helper()
	if scenario.directSend {
		return runDHTDispatchSendDirectScenario(t, scenario)
	}
	events := make([]string, 0, 3)
	state := dhtDispatchSendMutationState{Mutations: []string{}}
	responder := &dhtDispatchSendScriptedResponder{scenario: scenario, events: &events, state: &state}
	socket := &dhtDispatchSendCaptureSocket{
		events: &events, state: &state, err: scenario.socketErr, panicValue: scenario.socketPanic,
	}
	logCore, observedLogs := observer.New(zap.DebugLevel)
	logger := zap.New(logCore).Sugar()
	srv := &server{
		socket: socket, responder: responder, responderTimeout: scenario.responderTimeout, logger: logger,
	}
	parent, cleanup := dhtDispatchSendParentContext(scenario.contextKind)
	defer cleanup()
	received := dht.RecvMsg{Msg: scenario.request, From: scenario.source}
	terminal, recovered := invokeDHTDispatchSendHandleQuery(srv, parent, received)

	if responder.calls != 1 {
		t.Fatalf("%s: responder calls = %d, want 1", scenario.id, responder.calls)
	}
	inputExact := reflect.DeepEqual(responder.observed, received)
	if !inputExact {
		t.Fatalf("%s: handleQuery changed the responder input: got %#v want %#v", scenario.id, responder.observed, received)
	}
	if !responder.deadlinePresent {
		t.Fatalf("%s: responder context has no deadline", scenario.id)
	}
	if responder.contextAtRespond != scenario.wantContextAtRespond {
		t.Fatalf("%s: context at Respond = %q, want %q", scenario.id, responder.contextAtRespond, scenario.wantContextAtRespond)
	}
	contextAfter := dhtDispatchSendContextError(responder.context.Err())
	if contextAfter != scenario.wantContextAfter {
		t.Fatalf("%s: context after handleQuery = %q, want %q", scenario.id, contextAfter, scenario.wantContextAfter)
	}
	if terminal != scenario.wantTerminal {
		t.Fatalf("%s: terminal = %q, want %q (panic %#v)", scenario.id, terminal, scenario.wantTerminal, recovered)
	}
	if !reflect.DeepEqual(events, scenario.wantEvents) {
		t.Fatalf("%s: events = %#v, want %#v", scenario.id, events, scenario.wantEvents)
	}
	if len(socket.wires) != scenario.wantSendCalls {
		t.Fatalf("%s: Send calls = %d, want %d", scenario.id, len(socket.wires), scenario.wantSendCalls)
	}
	if len(socket.destinations) != len(socket.wires) {
		t.Fatalf("%s: destination/wire call count differs", scenario.id)
	}

	logs := projectDHTDispatchSendLogs(observedLogs, responder.returnedErr, scenario.socketErr)
	serverLogs, sendLogs := 0, 0
	for _, entry := range logs {
		switch entry.Message {
		case "server error":
			serverLogs++
			if entry.Level != "error" {
				t.Fatalf("%s: server error log level = %q", scenario.id, entry.Level)
			}
		case "could not send response":
			sendLogs++
			if entry.Level != "debug" {
				t.Fatalf("%s: send error log level = %q", scenario.id, entry.Level)
			}
		default:
			t.Fatalf("%s: unexpected server log %#v", scenario.id, entry)
		}
		if !entry.RetErrKey {
			t.Fatalf("%s: log %q omitted retErr field", scenario.id, entry.Message)
		}
	}
	if serverLogs != scenario.wantServerLogs || sendLogs != scenario.wantSendLogs {
		t.Fatalf(
			"%s: logs server=%d send=%d, want server=%d send=%d: %#v",
			scenario.id, serverLogs, sendLogs, scenario.wantServerLogs, scenario.wantSendLogs, logs,
		)
	}
	for _, entry := range logs {
		if entry.Message == "server error" && !entry.RetErrIdentity {
			t.Fatalf("%s: generic responder log lost exact error identity", scenario.id)
		}
		if entry.Message == "could not send response" && scenario.socketErr != nil && !entry.RetErrIdentity {
			t.Fatalf("%s: transport log lost exact error identity", scenario.id)
		}
	}

	panicText := dhtDispatchSendPanicText(recovered)
	panicIdentity := recovered != nil && (recovered == scenario.responderPanic || recovered == scenario.socketPanic)
	if panicText != scenario.wantPanicText {
		t.Fatalf("%s: panic text = %q, want %q", scenario.id, panicText, scenario.wantPanicText)
	}
	if panicIdentity != scenario.wantPanicIdentity {
		t.Fatalf("%s: panic identity = %v, want %v", scenario.id, panicIdentity, scenario.wantPanicIdentity)
	}

	var destination *dhtDispatchSendAddr
	var wireHex string
	var envelope *dhtDispatchSendEnvelope
	if len(socket.wires) == 1 {
		if socket.destinations[0] != scenario.source {
			t.Fatalf("%s: destination = %s, want exact source %s", scenario.id, socket.destinations[0], scenario.source)
		}
		projectedDestination := projectDHTDispatchSendAddr(socket.destinations[0])
		destination = &projectedDestination
		wireHex = hex.EncodeToString(socket.wires[0])
		projectedEnvelope := projectDHTDispatchSendEnvelope(t, socket.wires[0], scenario.request.T)
		envelope = &projectedEnvelope
	}
	assertDHTDispatchSendClassification(t, scenario, envelope)

	before := []string{}
	atSend := append([]string{}, socket.stateAtSend...)
	after := append([]string{}, state.Mutations...)
	if scenario.mutation != "" {
		want := []string{scenario.mutation}
		if !reflect.DeepEqual(atSend, want) || !reflect.DeepEqual(after, want) {
			t.Fatalf("%s: mutation state before send/after = %#v/%#v, want %#v", scenario.id, atSend, after, want)
		}
	} else if len(atSend) != 0 || len(after) != 0 {
		t.Fatalf("%s: unexpected mutation state %#v/%#v", scenario.id, atSend, after)
	}

	partialDiscarded := scenario.responseErr != nil || scenario.returnsContextError
	if partialDiscarded && envelope != nil && envelope.Return != nil {
		t.Fatalf("%s: partial responder return leaked into error envelope", scenario.id)
	}
	sendFailureSwallowed := terminal == "returned" && scenario.socketErr != nil
	return dhtDispatchSendFixture{
		ID: scenario.id, Subsystem: "dht_dispatch_send",
		Runtime: dhtDispatchSendRuntime{IntBits: strconv.IntSize},
		Input: dhtDispatchSendInput{
			Source:  projectDHTDispatchSendAddr(scenario.source),
			Request: projectDHTDispatchSendRequest(scenario.request),
			Context: scenario.contextKind,
			Responder: dhtDispatchSendResponderInput{
				Kind:          scenario.responseErrKind,
				Return:        projectDHTDispatchSendReturn(&scenario.response, dhtDispatchSendPresenceFromReturn(&scenario.response)),
				ErrorCode:     dhtDispatchSendInputErrorCode(scenario.responseErr),
				ErrorHex:      dhtDispatchSendInputErrorHex(scenario.responseErr),
				Mutation:      scenario.mutation,
				UnsupportedV:  scenario.unsupportedV,
				ReturnsCtxErr: scenario.returnsContextError,
				Panics:        scenario.responderPanic != nil,
			},
			Socket: dhtDispatchSendSocketInput{Kind: dhtDispatchSendSocketKind(scenario)},
			State:  dhtDispatchSendMutationState{Mutations: before},
		},
		Expected: dhtDispatchSendExpected{
			ResponderCalls: responder.calls, ResponderInputExact: inputExact,
			Context: dhtDispatchSendContext{
				DeadlinePresent: responder.deadlinePresent,
				ErrAtRespond:    responder.contextAtRespond,
				ErrAfter:        contextAfter,
			},
			Classification: scenario.wantClassification,
			Destination:    destination, WireHex: wireHex, Envelope: envelope,
			Events: append([]string(nil), events...), SendCalls: len(socket.wires), Logs: logs,
			State:                  dhtDispatchSendExpectedState{Before: before, AtSend: atSend, After: after},
			PartialReturnDiscarded: partialDiscarded,
			SendFailureSwallowed:   sendFailureSwallowed,
			Terminal:               terminal, PanicText: panicText, PanicIdentityExact: panicIdentity,
		},
	}
}

func runDHTDispatchSendDirectScenario(
	t *testing.T,
	scenario *dhtDispatchSendScenario,
) dhtDispatchSendFixture {
	t.Helper()
	events := make([]string, 0)
	state := dhtDispatchSendMutationState{Mutations: []string{}}
	socket := &dhtDispatchSendCaptureSocket{events: &events, state: &state}
	srv := &server{socket: socket}
	returnedErr, terminal, recovered := invokeDHTDispatchSendDirect(
		srv,
		scenario.source,
		scenario.directMessage,
	)
	if recovered != nil || terminal != scenario.wantTerminal {
		t.Fatalf("%s: direct send terminal=%q panic=%#v", scenario.id, terminal, recovered)
	}
	if returnedErr == nil {
		t.Fatalf("%s: direct send unexpectedly succeeded", scenario.id)
	}
	if got, want := fmt.Sprintf("%T", returnedErr), "*bencode.MarshalTypeError"; got != want {
		t.Fatalf("%s: returned encode error type = %q, want %q", scenario.id, got, want)
	}
	if got, want := returnedErr.Error(), "bencode: unsupported type: float64"; got != want {
		t.Fatalf("%s: returned encode error = %q, want %q", scenario.id, got, want)
	}
	if len(socket.wires) != 0 || len(events) != 0 {
		t.Fatalf("%s: direct encode error touched socket: calls=%d events=%#v", scenario.id, len(socket.wires), events)
	}
	empty := []string{}
	return dhtDispatchSendFixture{
		ID: scenario.id, Subsystem: "dht_dispatch_send",
		Runtime: dhtDispatchSendRuntime{IntBits: strconv.IntSize},
		Input: dhtDispatchSendInput{
			Source:  projectDHTDispatchSendAddr(scenario.source),
			Request: projectDHTDispatchSendRequest(scenario.directMessage),
			Context: scenario.contextKind,
			Responder: dhtDispatchSendResponderInput{
				Kind: scenario.responseErrKind,
				Return: projectDHTDispatchSendReturn(
					&scenario.response,
					dhtDispatchSendPresenceFromReturn(&scenario.response),
				),
				UnsupportedV: true,
			},
			Socket: dhtDispatchSendSocketInput{Kind: "success"},
			State:  dhtDispatchSendMutationState{Mutations: empty},
		},
		Expected: dhtDispatchSendExpected{
			ResponderCalls: 0, ResponderInputExact: false,
			Context:        dhtDispatchSendContext{DeadlinePresent: false, ErrAtRespond: "none", ErrAfter: "none"},
			Classification: scenario.wantClassification,
			Events:         empty, SendCalls: 0, Logs: []dhtDispatchSendLog{},
			State: dhtDispatchSendExpectedState{Before: empty, AtSend: empty, After: empty},
			ReturnedError: &dhtDispatchSendReturnedError{
				Type: fmt.Sprintf("%T", returnedErr), Text: returnedErr.Error(),
			},
			Terminal: terminal,
		},
	}
}

func invokeDHTDispatchSendDirect(
	srv *server,
	destination netip.AddrPort,
	message dht.Msg,
) (returnedErr error, terminal string, recovered any) {
	defer func() {
		if value := recover(); value != nil {
			terminal = "panicked"
			recovered = value
		}
	}()
	returnedErr = srv.send(destination, message)
	if returnedErr != nil {
		return returnedErr, "direct_send_returned", nil
	}
	return nil, "returned", nil
}

func invokeDHTDispatchSendHandleQuery(
	srv *server,
	ctx context.Context,
	received dht.RecvMsg,
) (terminal string, recovered any) {
	defer func() {
		if value := recover(); value != nil {
			terminal = "panicked"
			recovered = value
		}
	}()
	srv.handleQuery(ctx, received)
	return "returned", nil
}

func dhtDispatchSendParentContext(kind string) (context.Context, context.CancelFunc) {
	switch kind {
	case "active", "expired":
		return context.Background(), func() {}
	case "cancelled":
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, func() {}
	default:
		panic("unknown context kind: " + kind)
	}
}

func assertDHTDispatchSendClassification(
	t *testing.T,
	scenario *dhtDispatchSendScenario,
	envelope *dhtDispatchSendEnvelope,
) {
	t.Helper()
	switch scenario.wantClassification {
	case "success":
		if envelope == nil || envelope.Return == nil || envelope.Error != nil {
			t.Fatalf("%s: expected success envelope, got %#v", scenario.id, envelope)
		}
	case "protocol_203":
		assertDHTDispatchSendErrorEnvelope(t, scenario.id, envelope, 203, "missing arguments")
	case "protocol_204":
		assertDHTDispatchSendErrorEnvelope(t, scenario.id, envelope, 204, "method Unknown")
	case "generic_202":
		assertDHTDispatchSendErrorEnvelope(t, scenario.id, envelope, 202, "server error")
	case "success_encode_panic", "responder_panic":
		if envelope != nil {
			t.Fatalf("%s: unexpected sent envelope %#v", scenario.id, envelope)
		}
	default:
		t.Fatalf("%s: unknown wanted classification %q", scenario.id, scenario.wantClassification)
	}
}

func assertDHTDispatchSendErrorEnvelope(
	t *testing.T,
	id string,
	envelope *dhtDispatchSendEnvelope,
	code int,
	message string,
) {
	t.Helper()
	if envelope == nil || envelope.Return != nil || envelope.Error == nil ||
		envelope.Error.Code != code || envelope.Error.MessageHex != hex.EncodeToString([]byte(message)) {
		t.Fatalf("%s: wrong error envelope: %#v", id, envelope)
	}
}

func projectDHTDispatchSendEnvelope(t *testing.T, wire []byte, requestTID string) dhtDispatchSendEnvelope {
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
	presence := dhtDispatchSendPresenceFromRaw(raw)
	envelope := dhtDispatchSendEnvelope{
		TIDHex:        hex.EncodeToString([]byte(decoded.T)),
		TypeHex:       hex.EncodeToString([]byte(decoded.Y)),
		Presence:      presence,
		Canonical:     bytes.Equal(canonical, wire),
		TIDEchoed:     decoded.T == requestTID,
		FieldsCleared: !presence.Query && !presence.Arguments && !presence.IP && !presence.ReadOnly && !presence.ClientID,
	}
	if decoded.R != nil {
		projected := projectDHTDispatchSendReturn(decoded.R, presence)
		envelope.Return = &projected
	}
	if decoded.E != nil {
		envelope.Error = &dhtDispatchSendError{
			Code: decoded.E.Code, MessageHex: hex.EncodeToString([]byte(decoded.E.Msg)),
		}
	}
	if decoded.Y != dht.YResponse || !envelope.Canonical || !envelope.TIDEchoed || !envelope.FieldsCleared {
		t.Fatalf("sent envelope invariant changed: %#v", envelope)
	}
	return envelope
}

func dhtDispatchSendPresenceFromRaw(raw map[string]interface{}) dhtDispatchSendWirePresence {
	presence := dhtDispatchSendWirePresence{}
	_, presence.Query = raw["q"]
	_, presence.Arguments = raw["a"]
	_, presence.Return = raw["r"]
	_, presence.Error = raw["e"]
	_, presence.IP = raw["ip"]
	_, presence.ReadOnly = raw["ro"]
	_, presence.ClientID = raw["v"]
	if response, ok := raw["r"].(map[string]interface{}); ok {
		_, presence.ID = response["id"]
		_, presence.Nodes = response["nodes"]
		_, presence.Nodes6 = response["nodes6"]
		_, presence.Values = response["values"]
		_, presence.Token = response["token"]
		_, presence.Samples = response["samples"]
		_, presence.Num = response["num"]
		_, presence.Interval = response["interval"]
		_, presence.SeedersBF = response["BFsd"]
		_, presence.PeersBF = response["BFpe"]
	}
	return presence
}

func dhtDispatchSendPresenceFromReturn(ret *dht.Return) dhtDispatchSendWirePresence {
	return dhtDispatchSendWirePresence{
		Return: true, ID: true,
		Nodes: ret.Nodes != nil, Nodes6: ret.Nodes6 != nil, Values: ret.Values != nil,
		Token: ret.Token != nil, Samples: ret.Samples != nil, Num: ret.Num != nil,
		Interval: ret.Interval != nil, SeedersBF: ret.BFsd != nil, PeersBF: ret.BFpe != nil,
	}
}

func projectDHTDispatchSendReturn(
	ret *dht.Return,
	presence dhtDispatchSendWirePresence,
) dhtDispatchSendReturn {
	projected := dhtDispatchSendReturn{
		ID: ret.ID.String(), NodesPresent: presence.Nodes, Nodes6Present: presence.Nodes6,
		ValuesPresent: presence.Values, TokenPresent: presence.Token,
		SamplesPresent: presence.Samples, NumPresent: presence.Num, IntervalPresent: presence.Interval,
		Nodes: []dhtDispatchSendNode{}, Nodes6: []dhtDispatchSendNode{},
		Values: []dhtDispatchSendPeerAddr{}, Samples: []string{},
	}
	for _, node := range ret.Nodes {
		projected.Nodes = append(projected.Nodes, projectDHTDispatchSendNode(node))
	}
	for _, node := range ret.Nodes6 {
		projected.Nodes6 = append(projected.Nodes6, projectDHTDispatchSendNode(node))
	}
	for _, value := range ret.Values {
		projected.Values = append(projected.Values, projectDHTDispatchSendPeerAddr(value))
	}
	if ret.Token != nil {
		projected.TokenHex = hex.EncodeToString([]byte(*ret.Token))
	}
	if ret.Samples != nil {
		for _, sample := range *ret.Samples {
			projected.Samples = append(projected.Samples, sample.String())
		}
	}
	if ret.Num != nil {
		projected.Num = *ret.Num
	}
	if ret.Interval != nil {
		projected.Interval = *ret.Interval
	}
	return projected
}

func projectDHTDispatchSendNode(node dht.NodeInfo) dhtDispatchSendNode {
	return dhtDispatchSendNode{ID: node.ID.String(), Addr: projectDHTDispatchSendPeerAddr(node.Addr)}
}

func projectDHTDispatchSendPeerAddr(addr dht.NodeAddr) dhtDispatchSendPeerAddr {
	return dhtDispatchSendPeerAddr{IP: addr.IP.String(), Port: addr.Port}
}

func projectDHTDispatchSendAddr(addr netip.AddrPort) dhtDispatchSendAddr {
	scope := uint32(0)
	if addr.Addr().Zone() != "" {
		parsed, err := strconv.ParseUint(addr.Addr().Zone(), 10, 32)
		if err != nil {
			panic(err)
		}
		scope = uint32(parsed)
	}
	return dhtDispatchSendAddr{IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: scope}
}

func projectDHTDispatchSendRequest(msg dht.Msg) dhtDispatchSendRequest {
	return dhtDispatchSendRequest{
		TIDHex: hex.EncodeToString([]byte(msg.T)), TypeHex: hex.EncodeToString([]byte(msg.Y)),
		MethodHex: hex.EncodeToString([]byte(msg.Q)), ArgsPresent: msg.A != nil,
		MixedFields: msg.R != nil || msg.E != nil || len(msg.IP.IP) != 0 || msg.ReadOnly || msg.ClientID != "",
	}
}

func projectDHTDispatchSendLogs(
	logs *observer.ObservedLogs,
	responderErr error,
	socketErr error,
) []dhtDispatchSendLog {
	projected := make([]dhtDispatchSendLog, 0, 2)
	for _, entry := range logs.All() {
		if entry.Message != "server error" && entry.Message != "could not send response" {
			continue
		}
		value := interface{}(nil)
		keyPresent := false
		for _, field := range entry.Context {
			if field.Key == "retErr" {
				keyPresent = true
				value = field.Interface
			}
		}
		projected = append(projected, dhtDispatchSendLog{
			Level: entry.Level.String(), Message: entry.Message, RetErrKey: keyPresent,
			RetErrType: fmt.Sprintf("%T", value), RetErrText: dhtDispatchSendSafeErrorText(value),
			RetErrIdentity: dhtDispatchSendSameErrorValue(value, responderErr) ||
				dhtDispatchSendSameErrorValue(value, socketErr),
		})
	}
	return projected
}

func dhtDispatchSendSameErrorValue(value interface{}, expected error) bool {
	if value == nil || expected == nil {
		return false
	}
	actualType := reflect.TypeOf(value)
	if actualType != reflect.TypeOf(expected) || !actualType.Comparable() {
		return false
	}
	return value == expected
}

func dhtDispatchSendSafeErrorText(value interface{}) string {
	if value == nil {
		return ""
	}
	reflected := reflect.ValueOf(value)
	if (reflected.Kind() == reflect.Ptr || reflected.Kind() == reflect.Interface) && reflected.IsNil() {
		return "<typed nil " + reflected.Type().String() + ">"
	}
	if err, ok := value.(error); ok {
		return err.Error()
	}
	return fmt.Sprint(value)
}

func dhtDispatchSendContextError(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return err.Error()
	}
}

func dhtDispatchSendPanicText(value any) string {
	switch value := value.(type) {
	case nil:
		return ""
	case error:
		return value.Error()
	case string:
		return value
	default:
		return fmt.Sprint(value)
	}
}

func dhtDispatchSendInputErrorCode(err error) int {
	var value dht.Error
	if errors.As(err, &value) {
		return value.Code
	}
	var pointer *dht.Error
	if errors.As(err, &pointer) && pointer != nil {
		return pointer.Code
	}
	return 0
}

func dhtDispatchSendInputErrorHex(err error) string {
	if err == nil {
		return ""
	}
	reflected := reflect.ValueOf(err)
	if reflected.Kind() == reflect.Ptr && reflected.IsNil() {
		return ""
	}
	return hex.EncodeToString([]byte(err.Error()))
}

func dhtDispatchSendSocketKind(scenario *dhtDispatchSendScenario) string {
	switch {
	case scenario.socketPanic != nil:
		return "panic"
	case scenario.socketErr != nil:
		return "error"
	default:
		return "success"
	}
}

func dhtDispatchSendNodeInfo(last byte, addr string) dht.NodeInfo {
	return dht.NodeInfo{
		ID: dhtDispatchSendID(last), Addr: dht.NewNodeAddrFromAddrPort(netip.MustParseAddrPort(addr)),
	}
}

func dhtDispatchSendID(last byte) protocol.ID {
	var id protocol.ID
	id[19] = last
	return id
}

func assertDHTDispatchSendIndependentGoldens(t *testing.T, fixtures []dhtDispatchSendFixture) {
	t.Helper()
	goldens := map[string]string{
		"ping_success_empty_tid_mixed_request_y_ignored":   "64313a7264323a696432303a000000000000000000000000000000000000009065313a74303a313a79313a7265",
		"protocol_203_value_discards_partial_return":       "64313a656c693230336531373a6d697373696e6720617267756d656e747365313a74303a313a79313a7265",
		"generic_error_binary_tid_discards_partial_return": "64313a656c693230326531323a736572766572206572726f7265313a74323a00ff313a79313a7265",
	}
	seen := make(map[string]bool, len(fixtures))
	for _, fixture := range fixtures {
		if fixture.Runtime.IntBits != 64 || fixture.Subsystem != "dht_dispatch_send" {
			t.Fatalf("%s: unstable runtime/subsystem metadata", fixture.ID)
		}
		if seen[fixture.ID] {
			t.Fatalf("duplicate dispatch/send fixture ID %q", fixture.ID)
		}
		seen[fixture.ID] = true
		if golden, ok := goldens[fixture.ID]; ok && fixture.Expected.WireHex != golden {
			t.Fatalf("%s: independent golden changed:\n got %s\nwant %s", fixture.ID, fixture.Expected.WireHex, golden)
		}
	}
	if len(fixtures) != 26 || len(seen) != 26 {
		t.Fatalf("dispatch/send fixture matrix changed: rows=%d unique=%d, want 26", len(fixtures), len(seen))
	}
	for id := range goldens {
		if !seen[id] {
			t.Fatalf("independent golden case %q is absent", id)
		}
	}
}

func reconcileDHTDispatchSendFixtures(t *testing.T, fixtures []dhtDispatchSendFixture) {
	t.Helper()
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	for _, fixture := range fixtures {
		if err := encoder.Encode(fixture); err != nil {
			t.Fatal(err)
		}
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve generator source")
	}
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source), "../../../../testdata/parity/dht/dht_dispatch_send.jsonl",
	))
	if *updateDHTDispatchSendParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-dispatch-send-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT dispatch/send fixture is stale; rerun with -update-dht-dispatch-send-parity")
	}
}
