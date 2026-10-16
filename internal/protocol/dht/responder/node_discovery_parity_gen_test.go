package responder

import (
	"bytes"
	"context"
	"crypto/sha256"
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

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

var updateDHTResponderNodeDiscoveryParity = flag.Bool(
	"update-dht-responder-node-discovery-parity",
	false,
	"rewrite the Rust DHT responder node-discovery parity fixture",
)

const (
	nodeDiscoveryOrigin        = "00112233445566778899aabbccddeeff10203040"
	nodeDiscoveryInfoHash      = "11223344556677889900aabbccddeeff01020304"
	nodeDiscoveryTarget        = "0000000000000000000000000000000000000011"
	nodeDiscoveryFixtureSHA256 = "25cdc090696528dee56a89fec486b3ec36dd0021cc7c4d64844ad13c36c5918a"
)

var nodeDiscoveryFixtureIDs = [...]string{
	"ping_success_read_only_ipv4",
	"find_node_success_mapped_ipv4",
	"get_peers_success_scoped_ipv6",
	"announce_peer_success_mutates_before_notification",
	"sample_infohashes_success_native_ipv6",
	"ping_success_zero_requester_id",
	"duplicate_successes_are_preserved",
	"missing_arguments_suppresses_notification",
	"unknown_method_suppresses_notification",
	"missing_target_suppresses_notification",
	"invalid_announce_token_suppresses_notification",
}

type nodeDiscoveryFixture struct {
	ID        string                `json:"id"`
	Subsystem string                `json:"subsystem"`
	Oracle    nodeDiscoveryOracle   `json:"oracle"`
	Input     nodeDiscoveryInput    `json:"input"`
	Expected  nodeDiscoveryExpected `json:"expected"`
}

type nodeDiscoveryOracle struct {
	Composition               string `json:"composition"`
	Ingress                   string `json:"ingress"`
	ProductionSocketReachable bool   `json:"productionSocketReachable"`
}

type nodeDiscoveryInput struct {
	Method      string               `json:"method"`
	ArgsPresent bool                 `json:"argsPresent"`
	RequesterID string               `json:"requesterId"`
	InfoHash    string               `json:"infoHash,omitempty"`
	Target      string               `json:"target,omitempty"`
	Token       string               `json:"token,omitempty"`
	Source      nodeDiscoveryAddress `json:"source"`
	ReadOnly    bool                 `json:"readOnly"`
	Attempts    int                  `json:"attempts"`
}

type nodeDiscoveryExpected struct {
	Outcome                      string                `json:"outcome"`
	ReturnID                     string                `json:"returnId"`
	ProtocolError                *nodeDiscoveryError   `json:"protocolError,omitempty"`
	NoEventEvidence              string                `json:"noEventEvidence,omitempty"`
	RespondReturnedBeforeReceive bool                  `json:"respondReturnedBeforeReceive"`
	Events                       []nodeDiscoveryNode   `json:"events"`
	AnnounceStoredBeforeReceive  bool                  `json:"announceStoredBeforeReceive"`
	AnnouncePeerBeforeReceive    *nodeDiscoveryAddress `json:"announcePeerBeforeReceive,omitempty"`
}

type nodeDiscoveryError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type nodeDiscoveryNode struct {
	ID                          string               `json:"id"`
	Addr                        nodeDiscoveryAddress `json:"addr"`
	TimeZero                    bool                 `json:"timeZero"`
	Dropped                     bool                 `json:"dropped"`
	IsSampleInfoHashesCandidate bool                 `json:"isSampleInfoHashesCandidate"`
}

type nodeDiscoveryAddress struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope"`
}

func TestGenerateDHTResponderNodeDiscoveryParity(t *testing.T) {
	scenarios := []struct {
		id    string
		input nodeDiscoveryInput
	}{
		{
			"ping_success_read_only_ipv4",
			func() nodeDiscoveryInput {
				input := nodeDiscoveryRequest("ping", true, nodeDiscoveryID(1), "192.0.2.1:6881")
				input.ReadOnly = true
				return input
			}(),
		},
		{
			"find_node_success_mapped_ipv4",
			nodeDiscoveryRequest("find_node", true, nodeDiscoveryID(2), "[::ffff:192.0.2.2]:6882"),
		},
		{
			"get_peers_success_scoped_ipv6",
			nodeDiscoveryRequest("get_peers", true, nodeDiscoveryID(3), "[fe80::3%7]:6883"),
		},
		{
			"announce_peer_success_mutates_before_notification",
			nodeDiscoveryRequest("announce_peer", true, nodeDiscoveryID(4), "198.51.100.4:6884"),
		},
		{
			"sample_infohashes_success_native_ipv6",
			nodeDiscoveryRequest("sample_infohashes", true, nodeDiscoveryID(5), "[2001:db8::5]:6885"),
		},
		{
			"ping_success_zero_requester_id",
			nodeDiscoveryRequest("ping", true, protocol.ID{}.String(), "203.0.113.6:0"),
		},
		{
			"duplicate_successes_are_preserved",
			func() nodeDiscoveryInput {
				input := nodeDiscoveryRequest("ping", true, nodeDiscoveryID(7), "203.0.113.7:6887")
				input.Attempts = 2
				return input
			}(),
		},
		{
			"missing_arguments_suppresses_notification",
			nodeDiscoveryRequest("ping", false, protocol.ID{}.String(), "192.0.2.8:6888"),
		},
		{
			"unknown_method_suppresses_notification",
			nodeDiscoveryRequest("unknown", true, nodeDiscoveryID(9), "192.0.2.9:6889"),
		},
		{
			"missing_target_suppresses_notification",
			func() nodeDiscoveryInput {
				input := nodeDiscoveryRequest("find_node", true, nodeDiscoveryID(10), "192.0.2.10:6890")
				input.Target = ""
				return input
			}(),
		},
		{
			"invalid_announce_token_suppresses_notification",
			func() nodeDiscoveryInput {
				input := nodeDiscoveryRequest("announce_peer", true, nodeDiscoveryID(11), "192.0.2.11:6891")
				input.Token = "invalid"
				return input
			}(),
		},
	}

	fixtures := make([]nodeDiscoveryFixture, 0, len(scenarios))
	if len(scenarios) != len(nodeDiscoveryFixtureIDs) {
		t.Fatalf("scenario count = %d, want %d", len(scenarios), len(nodeDiscoveryFixtureIDs))
	}
	for index, scenario := range scenarios {
		if scenario.id != nodeDiscoveryFixtureIDs[index] {
			t.Fatalf("scenario %d id = %q, want %q", index, scenario.id, nodeDiscoveryFixtureIDs[index])
		}
		fixtures = append(fixtures, runNodeDiscoveryScenario(t, scenario.id, scenario.input))
	}
	reconcileNodeDiscoveryFixtures(t, fixtures)
}

func runNodeDiscoveryScenario(
	t *testing.T,
	id string,
	input nodeDiscoveryInput,
) nodeDiscoveryFixture {
	t.Helper()
	origin := protocol.MustParseID(nodeDiscoveryOrigin)
	table := ktable.New(ktable.Params{NodeID: origin}).Table
	core := responder{
		nodeID:                   origin,
		kTable:                   table,
		tokenSecret:              []byte("0123456789abcdefghij"),
		sampleInfoHashesInterval: 10,
	}
	discovered := make(chan ktable.Node)
	wrapper := responderNodeDiscovery{responder: core, discoveredNodes: discovered}
	msg := nodeDiscoveryMessage(t, core, input)
	expected := nodeDiscoveryExpected{Events: []nodeDiscoveryNode{}}

	for attempt := 0; attempt < input.Attempts; attempt++ {
		ret, err := wrapper.Respond(context.Background(), msg)
		expected.ReturnID = ret.ID.String()
		if err != nil {
			protocolErr, ok := err.(dht.Error)
			if !ok {
				t.Fatalf("%s: unexpected responder error type %T", id, err)
			}
			expected.Outcome = "protocol_error"
			expected.ProtocolError = &nodeDiscoveryError{Code: protocolErr.Code, Message: protocolErr.Msg}
			expected.NoEventEvidence = "source_predicate_err_non_nil"
			continue
		}

		expected.Outcome = "success"
		expected.RespondReturnedBeforeReceive = true
		if input.Method == dht.QAnnouncePeer {
			lookup := table.GetHashOrClosestNodes(protocol.MustParseID(nodeDiscoveryInfoHash))
			expected.AnnounceStoredBeforeReceive = lookup.Found && len(lookup.Hash.Peers()) == 1
			if expected.AnnounceStoredBeforeReceive {
				peer := projectNodeDiscoveryAddress(lookup.Hash.Peers()[0].Addr)
				expected.AnnouncePeerBeforeReceive = &peer
			}
		}
		select {
		case node := <-discovered:
			expected.Events = append(expected.Events, nodeDiscoveryNode{
				ID:                          node.ID().String(),
				Addr:                        projectNodeDiscoveryAddress(node.Addr()),
				TimeZero:                    node.Time().IsZero(),
				Dropped:                     node.Dropped(),
				IsSampleInfoHashesCandidate: node.IsSampleInfoHashesCandidate(),
			})
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: timed out receiving successful discovery attempt %d", id, attempt+1)
		}
	}

	return nodeDiscoveryFixture{
		ID:        id,
		Subsystem: "dht_responder_node_discovery",
		Oracle: nodeDiscoveryOracle{
			Composition:               "private_core_then_actual_node_discovery_wrapper",
			Ingress:                   "direct_recv_msg",
			ProductionSocketReachable: input.Source.addrPort(t).Addr().Is4(),
		},
		Input: input, Expected: expected,
	}
}

func nodeDiscoveryMessage(
	t *testing.T,
	core responder,
	input nodeDiscoveryInput,
) dht.RecvMsg {
	t.Helper()
	from := input.Source.addrPort(t)
	msg := dht.Msg{Q: input.Method, T: "ND", Y: dht.YQuery, ReadOnly: input.ReadOnly}
	if !input.ArgsPresent {
		return dht.RecvMsg{Msg: msg, From: from}
	}

	args := &dht.MsgArgs{ID: protocol.MustParseID(input.RequesterID)}
	if input.InfoHash != "" {
		args.InfoHash = protocol.MustParseID(input.InfoHash)
	}
	if input.Target != "" {
		args.Target = protocol.MustParseID(input.Target)
	}
	if input.Method == dht.QAnnouncePeer {
		port := 51_413
		args.Port = &port
		if input.Token == "valid" {
			args.Token = core.announceToken(args.InfoHash, args.ID, from.Addr())
		} else {
			args.Token = input.Token
		}
	}
	msg.A = args
	return dht.RecvMsg{Msg: msg, From: from}
}

func nodeDiscoveryRequest(
	method string,
	argsPresent bool,
	requesterID string,
	source string,
) nodeDiscoveryInput {
	input := nodeDiscoveryInput{
		Method: method, ArgsPresent: argsPresent, RequesterID: requesterID,
		Source: nodeDiscoveryAddressFromString(source), Attempts: 1,
	}
	switch method {
	case dht.QFindNode:
		input.Target = nodeDiscoveryTarget
	case dht.QGetPeers:
		input.InfoHash = nodeDiscoveryInfoHash
	case dht.QAnnouncePeer:
		input.InfoHash = nodeDiscoveryInfoHash
		input.Token = "valid"
	}
	return input
}

func nodeDiscoveryID(last byte) string {
	var id protocol.ID
	id[19] = last
	return id.String()
}

func nodeDiscoveryAddressFromString(value string) nodeDiscoveryAddress {
	addr, err := netip.ParseAddrPort(value)
	if err != nil {
		panic(err)
	}
	return projectNodeDiscoveryAddress(addr)
}

func projectNodeDiscoveryAddress(addr netip.AddrPort) nodeDiscoveryAddress {
	scope := uint64(0)
	if zone := addr.Addr().Zone(); zone != "" {
		var err error
		scope, err = strconv.ParseUint(zone, 10, 32)
		if err != nil {
			panic(err)
		}
	}
	return nodeDiscoveryAddress{
		IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: uint32(scope),
	}
}

func (a nodeDiscoveryAddress) addrPort(t *testing.T) netip.AddrPort {
	t.Helper()
	ip, err := netip.ParseAddr(a.IP)
	if err != nil {
		t.Fatal(err)
	}
	if a.Scope != 0 {
		ip = ip.WithZone(strconv.FormatUint(uint64(a.Scope), 10))
	}
	return netip.AddrPortFrom(ip, a.Port)
}

func reconcileNodeDiscoveryFixtures(t *testing.T, fixtures []nodeDiscoveryFixture) {
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
	fixtureHash := sha256.Sum256(encoded.Bytes())
	if actual := fmt.Sprintf("%x", fixtureHash); actual != nodeDiscoveryFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actual, nodeDiscoveryFixtureSHA256)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve node-discovery generator source")
	}
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source), "../../../../testdata/parity/dht/responder_node_discovery.jsonl",
	))
	if *updateDHTResponderNodeDiscoveryParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-responder-node-discovery-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("responder node-discovery fixture is stale; rerun with -update-dht-responder-node-discovery-parity")
	}
}
