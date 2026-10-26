package client

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
)

var updateDHTPingFindNodeClientParity = flag.Bool(
	"update-dht-ping-find-node-client-parity",
	false,
	"rewrite the Rust DHT ping/find-node client fixture",
)

type pingFindNodeClientFixture struct {
	ID        string                     `json:"id"`
	Subsystem string                     `json:"subsystem"`
	Input     pingFindNodeClientInput    `json:"input"`
	Expected  pingFindNodeClientExpected `json:"expected"`
}

type pingFindNodeClientInput struct {
	Method                 string                   `json:"method"`
	TransactionIDHex       string                   `json:"transactionIdHex"`
	LocalID                string                   `json:"localId"`
	Remote                 pingFindNodeClientAddr   `json:"remote"`
	Target                 *string                  `json:"target,omitempty"`
	ResponseID             string                   `json:"responseId"`
	ResponseNodesPresence  string                   `json:"responseNodesPresence"`
	ResponseNodes          []pingFindNodeClientNode `json:"responseNodes"`
	ResponseNodes6Presence string                   `json:"responseNodes6Presence"`
	ResponseNodes6         []pingFindNodeClientNode `json:"responseNodes6"`
	IncludeIgnoredFields   bool                     `json:"includeIgnoredFields,omitempty"`
	FailQuery              bool                     `json:"failQuery,omitempty"`
	PreCancelled           bool                     `json:"preCancelled,omitempty"`
	TypedNilError          bool                     `json:"typedNilError,omitempty"`
}

type pingFindNodeClientExpected struct {
	QueryCalls             int                      `json:"queryCalls"`
	QueryMethod            string                   `json:"queryMethod"`
	QueryLocalID           string                   `json:"queryLocalId"`
	QueryTarget            string                   `json:"queryTarget"`
	QueryRemote            pingFindNodeClientAddr   `json:"queryRemote"`
	QueryWireHex           string                   `json:"queryWireHex"`
	Outcome                string                   `json:"outcome"`
	ResultID               string                   `json:"resultId"`
	ResultNodes            []pingFindNodeClientNode `json:"resultNodes"`
	ErrorIdentityPreserved bool                     `json:"errorIdentityPreserved"`
	ErrorIsTypedNil        bool                     `json:"errorIsTypedNil,omitempty"`
	ResultWasZero          bool                     `json:"resultWasZero"`
}

type pingFindNodeClientAddr struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope,omitempty"`
}

type pingFindNodeClientNode struct {
	ID   string                 `json:"id"`
	Addr pingFindNodeClientAddr `json:"addr"`
}

type pingFindNodeClientScenario struct {
	id                     string
	method                 string
	transactionID          string
	localID                protocol.ID
	remote                 netip.AddrPort
	target                 protocol.ID
	responseID             protocol.ID
	responseNodesPresence  string
	responseNodes          []dht.NodeInfo
	responseNodes6Presence string
	responseNodes6         []dht.NodeInfo
	includeIgnoredFields   bool
	failQuery              bool
	preCancelled           bool
	typedNilError          bool
}

// Embedding the sealed production interface promotes its private lifecycle
// methods while this test overrides Query. That lets the oracle invoke the
// real unexported serverAdapter without opening a socket or changing source.
type pingFindNodeClientScriptedServer struct {
	server.Server
	transactionID string
	response      dht.RecvMsg
	queryErr      error
	queries       []pingFindNodeClientCapturedQuery
}

type pingFindNodeClientCapturedQuery struct {
	remote netip.AddrPort
	method string
	args   dht.MsgArgs
	wire   []byte
}

func (s *pingFindNodeClientScriptedServer) Query(
	ctx context.Context,
	remote netip.AddrPort,
	method string,
	args dht.MsgArgs,
) (dht.RecvMsg, error) {
	wire, err := bencode.Marshal(dht.Msg{
		T: s.transactionID,
		Y: dht.YQuery,
		Q: method,
		A: &args,
	})
	if err != nil {
		panic(err)
	}
	s.queries = append(s.queries, pingFindNodeClientCapturedQuery{
		remote: remote,
		method: method,
		args:   args,
		wire:   wire,
	})
	if err := ctx.Err(); err != nil {
		return dht.RecvMsg{}, err
	}
	return s.response, s.queryErr
}

func TestGenerateDHTPingFindNodeClientParity(t *testing.T) {
	localID := pingFindNodeClientID(0x11)
	target := pingFindNodeClientID(0x22)
	responseID := pingFindNodeClientID(0x33)
	nodeA := pingFindNodeClientNodeInfo(0x41, "192.0.2.40", 0)
	nodeB := pingFindNodeClientNodeInfo(0x42, "192.0.2.40", 65535)
	nodeMapped := pingFindNodeClientNodeInfo(0x43, "::ffff:192.0.2.41", 6881)
	nodeV6 := pingFindNodeClientNodeInfo(0x44, "2001:db8::44", 6882)

	scenarios := []pingFindNodeClientScenario{
		{
			id: "ping_ipv4_projects_only_id", method: dht.QPing, transactionID: "P1",
			localID: localID, remote: netip.MustParseAddrPort("192.0.2.1:6881"),
			responseID: responseID, responseNodesPresence: "present",
			responseNodes: []dht.NodeInfo{nodeA}, includeIgnoredFields: true,
		},
		{
			id: "ping_mapped_remote_accepts_zero_response_id", method: dht.QPing,
			transactionID: "P2", localID: protocol.ID{},
			remote:     netip.MustParseAddrPort("[::ffff:192.0.2.2]:0"),
			responseID: protocol.ID{},
		},
		{
			id: "find_node_preserves_order_and_duplicate_endpoints", method: dht.QFindNode,
			transactionID: "F1", localID: localID,
			remote: netip.MustParseAddrPort("[fe80::1%7]:6882"), target: target,
			responseID: responseID, responseNodesPresence: "present",
			responseNodes: []dht.NodeInfo{nodeA, nodeB, nodeA},
		},
		{
			id: "find_node_nil_nodes_become_empty", method: dht.QFindNode,
			transactionID: "F2", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.3:6883"), target: target,
			responseID: protocol.ID{},
		},
		{
			id: "find_node_present_empty_nodes_become_empty", method: dht.QFindNode,
			transactionID: "F3", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.4:6884"), target: target,
			responseID: responseID, responseNodesPresence: "empty",
		},
		{
			id: "find_node_ignores_nodes6_values_and_extensions", method: dht.QFindNode,
			transactionID: "F4", localID: localID,
			remote: netip.MustParseAddrPort("[2001:db8::5]:6885"), target: target,
			responseID: responseID, responseNodes6Presence: "present",
			responseNodes6: []dht.NodeInfo{nodeV6}, includeIgnoredFields: true,
		},
		{
			id:     "find_node_typed_nodes_preserve_mapped_and_native_addresses",
			method: dht.QFindNode, transactionID: "F5", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.6:6886"), target: target,
			responseID: responseID, responseNodesPresence: "present",
			responseNodes: []dht.NodeInfo{nodeMapped, nodeV6},
		},
		{
			id:     "find_node_zero_target_is_captured_and_omitted_from_wire",
			method: dht.QFindNode, transactionID: "F6", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.7:6887"), target: protocol.ID{},
			responseID: responseID,
		},
		{
			id: "query_error_identity_wins_and_zeroes_result", method: dht.QPing,
			transactionID: "E1", localID: localID,
			remote:     netip.MustParseAddrPort("192.0.2.8:6888"),
			responseID: responseID, responseNodesPresence: "present",
			responseNodes: []dht.NodeInfo{nodeA}, failQuery: true,
		},
		{
			id: "pre_cancelled_context_still_invokes_query", method: dht.QPing,
			transactionID: "C1", localID: localID,
			remote:     netip.MustParseAddrPort("192.0.2.9:6889"),
			responseID: responseID, preCancelled: true,
		},
		{
			id: "missing_error_body_surfaces_as_typed_nil_go_error", method: dht.QPing,
			transactionID: "M1", localID: localID,
			remote:     netip.MustParseAddrPort("192.0.2.10:6890"),
			responseID: responseID, typedNilError: true,
		},
	}

	fixtures := make([]pingFindNodeClientFixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixtures = append(fixtures, runPingFindNodeClientScenario(t, scenario))
	}
	reconcilePingFindNodeClientFixtures(t, fixtures)
}

func runPingFindNodeClientScenario(
	t *testing.T,
	scenario pingFindNodeClientScenario,
) pingFindNodeClientFixture {
	t.Helper()
	response := dht.Return{ID: scenario.responseID}
	switch scenario.responseNodesPresence {
	case "":
	case "empty":
		response.Nodes = dht.CompactIPv4NodeInfo{}
	case "present":
		response.Nodes = append(dht.CompactIPv4NodeInfo(nil), scenario.responseNodes...)
	default:
		t.Fatalf("%s: unknown nodes presence %q", scenario.id, scenario.responseNodesPresence)
	}
	switch scenario.responseNodes6Presence {
	case "":
	case "empty":
		response.Nodes6 = dht.CompactIPv6NodeInfo{}
	case "present":
		response.Nodes6 = append(dht.CompactIPv6NodeInfo(nil), scenario.responseNodes6...)
	default:
		t.Fatalf("%s: unknown nodes6 presence %q", scenario.id, scenario.responseNodes6Presence)
	}
	if scenario.includeIgnoredFields {
		token := "ignored-token"
		interval := int64(17)
		num := int64(19)
		samples := dht.CompactInfohashes{pingFindNodeClientID(0x55)}
		response.Token = &token
		response.Values = []dht.NodeAddr{pingFindNodeClientNodeAddr("198.51.100.9", 9999)}
		response.Interval = &interval
		response.Num = &num
		response.Samples = &samples
	}

	sentinel := errors.New("ping/find-node client oracle sentinel")
	scripted := &pingFindNodeClientScriptedServer{
		transactionID: scenario.transactionID,
		response: dht.RecvMsg{
			From: scenario.remote,
			Msg:  dht.Msg{T: scenario.transactionID, Y: dht.YResponse, R: &response},
		},
	}
	if scenario.failQuery {
		scripted.queryErr = sentinel
	} else if scenario.typedNilError {
		var typedNil *dht.Error
		scripted.queryErr = typedNil
	}
	adapter := serverAdapter{nodeID: scenario.localID, server: scripted}
	ctx := context.Background()
	if scenario.preCancelled {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	}

	var resultID protocol.ID
	var resultNodes []NodeInfo
	var resultWasZero bool
	var err error
	switch scenario.method {
	case dht.QPing:
		var result PingResult
		result, err = adapter.Ping(ctx, scenario.remote)
		resultID = result.ID
		resultWasZero = result == (PingResult{})
	case dht.QFindNode:
		var result FindNodeResult
		result, err = adapter.FindNode(ctx, scenario.remote, scenario.target)
		resultID = result.ID
		resultNodes = result.Nodes
		resultWasZero = result.ID.IsZero() && result.Nodes == nil
	default:
		t.Fatalf("%s: unsupported method %q", scenario.id, scenario.method)
	}
	if len(scripted.queries) != 1 {
		t.Fatalf("%s: expected one Query call, got %d", scenario.id, len(scripted.queries))
	}
	captured := scripted.queries[0]
	expectedArgs := dht.MsgArgs{ID: scenario.localID}
	if scenario.method == dht.QFindNode {
		expectedArgs.Target = scenario.target
	}
	if !reflect.DeepEqual(captured.args, expectedArgs) {
		t.Fatalf("%s: exact query args changed: got %#v want %#v", scenario.id, captured.args, expectedArgs)
	}
	if scenario.failQuery {
		if err != sentinel || !resultWasZero {
			t.Fatalf("%s: sentinel identity/result zero contract changed", scenario.id)
		}
	} else if scenario.preCancelled {
		if !errors.Is(err, context.Canceled) || !resultWasZero {
			t.Fatalf("%s: pre-cancelled call/result contract changed", scenario.id)
		}
	} else if scenario.typedNilError {
		if err == nil || !reflect.ValueOf(err).IsNil() || !resultWasZero {
			t.Fatalf("%s: typed-nil error/result contract changed", scenario.id)
		}
	} else if err != nil {
		t.Fatalf("%s: unexpected adapter error: %v", scenario.id, err)
	}

	input := pingFindNodeClientInput{
		Method: scenario.method, TransactionIDHex: hex.EncodeToString([]byte(scenario.transactionID)),
		LocalID: scenario.localID.String(), Remote: pingFindNodeClientProjectAddr(scenario.remote),
		ResponseID:             scenario.responseID.String(),
		ResponseNodesPresence:  scenario.responseNodesPresence,
		ResponseNodes:          pingFindNodeClientProjectNodes(scenario.responseNodes),
		ResponseNodes6Presence: scenario.responseNodes6Presence,
		ResponseNodes6:         pingFindNodeClientProjectNodes(scenario.responseNodes6),
		IncludeIgnoredFields:   scenario.includeIgnoredFields, FailQuery: scenario.failQuery,
		PreCancelled: scenario.preCancelled, TypedNilError: scenario.typedNilError,
	}
	if scenario.method == dht.QFindNode {
		target := scenario.target.String()
		input.Target = &target
	}
	expected := pingFindNodeClientExpected{
		QueryCalls: len(scripted.queries), QueryMethod: captured.method,
		QueryLocalID: captured.args.ID.String(), QueryTarget: captured.args.Target.String(),
		QueryRemote:  pingFindNodeClientProjectAddr(captured.remote),
		QueryWireHex: hex.EncodeToString(captured.wire),
		Outcome:      "success", ResultID: resultID.String(),
		ResultNodes:            pingFindNodeClientProjectResultNodes(resultNodes),
		ErrorIdentityPreserved: false, ResultWasZero: resultWasZero,
	}
	if scenario.failQuery {
		expected.Outcome = "query_error"
		expected.ErrorIdentityPreserved = err == sentinel
	} else if scenario.preCancelled {
		expected.Outcome = "context_cancelled"
	} else if scenario.typedNilError {
		expected.Outcome = "typed_nil_error"
		expected.ErrorIsTypedNil = err != nil && reflect.ValueOf(err).IsNil()
	}
	return pingFindNodeClientFixture{
		ID: scenario.id, Subsystem: "dht_ping_find_node_client", Input: input, Expected: expected,
	}
}

func pingFindNodeClientID(last byte) protocol.ID {
	var id protocol.ID
	id[19] = last
	return id
}

func pingFindNodeClientNodeInfo(last byte, ip string, port uint16) dht.NodeInfo {
	return dht.NodeInfo{
		ID:   pingFindNodeClientID(last),
		Addr: pingFindNodeClientNodeAddr(ip, port),
	}
}

func pingFindNodeClientNodeAddr(ip string, port uint16) dht.NodeAddr {
	addr := netip.MustParseAddr(ip)
	return dht.NodeAddr{IP: net.IP(addr.AsSlice()), Port: int(port)}
}

func pingFindNodeClientProjectResultNodes(nodes []NodeInfo) []pingFindNodeClientNode {
	if nodes == nil {
		return nil
	}
	projected := make([]pingFindNodeClientNode, 0, len(nodes))
	for _, node := range nodes {
		projected = append(projected, pingFindNodeClientNode{
			ID: node.ID.String(), Addr: pingFindNodeClientProjectAddr(node.Addr),
		})
	}
	return projected
}

func pingFindNodeClientProjectNodes(nodes []dht.NodeInfo) []pingFindNodeClientNode {
	if nodes == nil {
		return nil
	}
	projected := make([]pingFindNodeClientNode, 0, len(nodes))
	for _, node := range nodes {
		projected = append(projected, pingFindNodeClientNode{
			ID: node.ID.String(), Addr: pingFindNodeClientProjectAddr(node.Addr.ToAddrPort()),
		})
	}
	return projected
}

func pingFindNodeClientProjectAddr(addr netip.AddrPort) pingFindNodeClientAddr {
	scope := uint32(0)
	if addr.Addr().Zone() != "" {
		parsed, err := strconv.ParseUint(addr.Addr().Zone(), 10, 32)
		if err != nil {
			panic(err)
		}
		scope = uint32(parsed)
	}
	return pingFindNodeClientAddr{
		IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: scope,
	}
}

func reconcilePingFindNodeClientFixtures(t *testing.T, fixtures []pingFindNodeClientFixture) {
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
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source), "../../../../testdata/parity/dht/ping_find_node_client.jsonl",
	))
	if *updateDHTPingFindNodeClientParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-ping-find-node-client-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("ping/find-node client fixture is stale; rerun with -update-dht-ping-find-node-client-parity")
	}
}
