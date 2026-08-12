package responder

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
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

var updateDHTPingFindNodeParity = flag.Bool(
	"update-dht-ping-find-node-parity",
	false,
	"rewrite the Rust DHT ping/find-node responder parity fixture",
)

type pingFindNodeFixture struct {
	ID        string               `json:"id"`
	Subsystem string               `json:"subsystem"`
	Input     pingFindNodeInput    `json:"input"`
	Expected  pingFindNodeExpected `json:"expected"`
}

type pingFindNodeInput struct {
	Origin  string              `json:"origin"`
	Nodes   []pingFindNodeNode  `json:"nodes"`
	Request pingFindNodeRequest `json:"request"`
}

type pingFindNodeRequest struct {
	Method        string   `json:"method"`
	ArgsPresent   bool     `json:"argsPresent"`
	SenderID      string   `json:"senderId,omitempty"`
	TargetPresent bool     `json:"targetPresent,omitempty"`
	Target        string   `json:"target,omitempty"`
	Want          []string `json:"want,omitempty"`
}

type pingFindNodeExpected struct {
	RustOutcome    string               `json:"rustOutcome"`
	Response       pingFindNodeResponse `json:"goResponse"`
	ProtocolError  *pingFindNodeError   `json:"protocolError,omitempty"`
	NativeIPv6Node *pingFindNodeNode    `json:"nativeIpv6Node,omitempty"`
	WireHex        string               `json:"wireHex,omitempty"`
	GoWirePanicked bool                 `json:"goWirePanicked,omitempty"`
}

type pingFindNodeResponse struct {
	ID    string             `json:"id"`
	Nodes []pingFindNodeNode `json:"nodes,omitempty"`
}

type pingFindNodeError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type pingFindNodeNode struct {
	ID   string              `json:"id"`
	Addr pingFindNodeAddress `json:"addr"`
}

type pingFindNodeAddress struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope,omitempty"`
}

func TestGenerateDHTPingFindNodeParity(t *testing.T) {
	zero := protocol.ID{}.String()
	origin := responderID(0x90)
	fullNodes := make([]pingFindNodeNode, 80)
	for index := range fullNodes {
		id := protocol.ID{0xc0}
		id[18] = byte((index + 1) >> 8)
		id[19] = byte(index + 1)
		fullNodes[index] = responderNode(id.String(), "203.0.113.80", uint16(index), 0)
	}

	scenarios := []struct {
		id    string
		input pingFindNodeInput
	}{
		{"ping_missing_arguments", responderInput(origin, nil, request("ping", false, zero, false, ""))},
		{"ping_zero_sender_id", responderInput(origin, nil, request("ping", true, zero, false, ""))},
		{"ping_ignores_target_and_want", responderInput(origin, nil, requestWithWant("ping", true, responderID(1), true, responderID(2), []string{"n6", "n4"}))},
		{"find_node_missing_arguments", responderInput(origin, nil, request("find_node", false, zero, false, ""))},
		{"find_node_missing_target", responderInput(origin, nil, request("find_node", true, responderID(1), false, ""))},
		{"find_node_zero_target", responderInput(origin, nil, request("find_node", true, responderID(1), true, zero))},
		{"find_node_empty_table", responderInput(origin, nil, request("find_node", true, responderID(1), true, responderID(2)))},
		{"find_node_exact_ipv4_port_zero", responderInput(origin, []pingFindNodeNode{
			responderNode(responderID(2), "192.0.2.1", 0, 0),
		}, request("find_node", true, responderID(1), true, responderID(2)))},
		{"find_node_exact_mapped_ipv4", responderInput(origin, []pingFindNodeNode{
			responderNode(responderID(2), "::ffff:192.0.2.2", 6881, 0),
		}, requestWithWant("find_node", true, responderID(1), true, responderID(2), []string{"n6"}))},
		{"find_node_full_table_closest_eight", responderInput(zero, fullNodes, request("find_node", true, responderID(1), true, responderID(0x7f)))},
		{"find_node_origin_target", responderInput(origin, []pingFindNodeNode{
			responderNode(responderID(1), "192.0.2.10", 1, 0),
			responderNode(responderID(2), "192.0.2.10", 2, 0),
		}, request("find_node", true, responderID(3), true, origin))},
		{"find_node_native_ipv6", responderInput(origin, []pingFindNodeNode{
			responderNode(responderID(2), "2001:db8::2", 6881, 0),
		}, request("find_node", true, responderID(1), true, responderID(2)))},
		{"find_node_scoped_native_ipv6", responderInput(origin, []pingFindNodeNode{
			responderNode(responderID(2), "fe80::2", 6881, 7),
		}, request("find_node", true, responderID(1), true, responderID(2)))},
		{"find_node_mixed_native_ipv6", responderInput(origin, []pingFindNodeNode{
			responderNode(responderID(1), "192.0.2.3", 3, 0),
			responderNode(responderID(2), "2001:db8::3", 4, 0),
		}, request("find_node", true, responderID(4), true, responderID(3)))},
	}

	fixtures := make([]pingFindNodeFixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixtures = append(fixtures, runPingFindNodeScenario(t, scenario.id, scenario.input))
	}
	reconcilePingFindNodeFixtures(t, fixtures)
}

func runPingFindNodeScenario(t *testing.T, id string, input pingFindNodeInput) pingFindNodeFixture {
	t.Helper()
	table := ktable.New(ktable.Params{NodeID: protocol.MustParseID(input.Origin)}).Table
	for _, node := range input.Nodes {
		if result := table.PutNode(protocol.MustParseID(node.ID), node.Addr.addrPort()); result.String() != "accepted" {
			t.Fatalf("%s: seed %s: %s", id, node.ID, result)
		}
	}
	requestMessage := dht.Msg{Q: input.Request.Method, T: string([]byte{1, 2}), Y: dht.YQuery}
	if input.Request.ArgsPresent {
		args := dht.MsgArgs{ID: protocol.MustParseID(input.Request.SenderID)}
		if input.Request.TargetPresent {
			args.Target = protocol.MustParseID(input.Request.Target)
		}
		for _, want := range input.Request.Want {
			args.Want = append(args.Want, dht.Want(want))
		}
		requestMessage.A = &args
	}
	actualResponder := responder{nodeID: protocol.MustParseID(input.Origin), kTable: table}
	response, responseErr := actualResponder.Respond(context.Background(), dht.RecvMsg{
		Msg:  requestMessage,
		From: netip.MustParseAddrPort("198.51.100.1:6881"),
	})

	expected := pingFindNodeExpected{Response: projectPingFindNodeResponse(response)}
	if responseErr != nil {
		dhtErr, ok := responseErr.(dht.Error)
		if !ok {
			t.Fatalf("%s: unexpected responder error %T: %v", id, responseErr, responseErr)
		}
		expected.RustOutcome = "protocol"
		expected.ProtocolError = &pingFindNodeError{Code: dhtErr.Code, Message: hex.EncodeToString([]byte(dhtErr.Msg))}
	} else if native := firstNativeIPv6(input, response); native != nil {
		expected.RustOutcome = "nativeIpv6Node"
		expected.NativeIPv6Node = native
	} else {
		expected.RustOutcome = "ok"
	}

	wire, panicked, err := marshalPingFindNodeEnvelope(response, responseErr)
	if err != nil {
		t.Fatalf("%s: marshal response envelope: %v", id, err)
	}
	expected.GoWirePanicked = panicked
	if !panicked {
		expected.WireHex = hex.EncodeToString(wire)
	}
	if expected.RustOutcome == "nativeIpv6Node" && !panicked {
		t.Fatalf("%s: native IPv6 response unexpectedly encoded", id)
	}
	return pingFindNodeFixture{ID: id, Subsystem: "dht_ping_find_node", Input: input, Expected: expected}
}

func marshalPingFindNodeEnvelope(response dht.Return, responseErr error) (wire []byte, panicked bool, err error) {
	defer func() {
		if recover() != nil {
			wire = nil
			panicked = true
			err = nil
		}
	}()
	message := dht.Msg{T: string([]byte{1, 2}), Y: dht.YResponse}
	if responseErr == nil {
		message.R = &response
	} else {
		dhtErr := responseErr.(dht.Error)
		message.E = &dhtErr
	}
	wire, err = bencode.Marshal(message)
	return
}

func projectPingFindNodeResponse(value dht.Return) pingFindNodeResponse {
	result := pingFindNodeResponse{ID: value.ID.String()}
	for _, node := range value.Nodes {
		result.Nodes = append(result.Nodes, pingFindNodeNode{
			ID: node.ID.String(),
			Addr: pingFindNodeAddress{
				IP:   node.Addr.IP.String(),
				Port: uint16(node.Addr.Port),
			},
		})
	}
	return result
}

func firstNativeIPv6(input pingFindNodeInput, response dht.Return) *pingFindNodeNode {
	for _, responseNode := range response.Nodes {
		addr, ok := netip.AddrFromSlice(responseNode.Addr.IP)
		if !ok || !addr.Is6() || addr.Is4In6() {
			continue
		}
		for _, inputNode := range input.Nodes {
			if inputNode.ID == responseNode.ID.String() {
				copy := inputNode
				return &copy
			}
		}
	}
	return nil
}

func (value pingFindNodeAddress) addrPort() netip.AddrPort {
	ip := netip.MustParseAddr(value.IP)
	if value.Scope != 0 {
		ip = ip.WithZone(fmt.Sprint(value.Scope))
	}
	return netip.AddrPortFrom(ip, value.Port)
}

func responderInput(origin string, nodes []pingFindNodeNode, req pingFindNodeRequest) pingFindNodeInput {
	return pingFindNodeInput{Origin: origin, Nodes: nodes, Request: req}
}

func request(method string, args bool, sender string, targetPresent bool, target string) pingFindNodeRequest {
	return requestWithWant(method, args, sender, targetPresent, target, nil)
}

func requestWithWant(method string, args bool, sender string, targetPresent bool, target string, want []string) pingFindNodeRequest {
	return pingFindNodeRequest{Method: method, ArgsPresent: args, SenderID: sender, TargetPresent: targetPresent, Target: target, Want: want}
}

func responderNode(id, ip string, port uint16, scope uint32) pingFindNodeNode {
	return pingFindNodeNode{ID: id, Addr: pingFindNodeAddress{IP: ip, Port: port, Scope: scope}}
}

func responderID(last byte) string {
	var id protocol.ID
	id[19] = last
	return id.String()
}

func reconcilePingFindNodeFixtures(t *testing.T, fixtures []pingFindNodeFixture) {
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
	path := filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../testdata/parity/dht/ping_find_node.jsonl"))
	if *updateDHTPingFindNodeParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-ping-find-node-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("ping/find-node fixture is stale; rerun with -update-dht-ping-find-node-parity")
	}
}
