package parity

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
)

const dhtKRPCWireSubsystem = "dht_krpc_wire"

type dhtBytes []byte

func (value dhtBytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(value))
}

type dhtCompactAddr struct {
	IP   string `json:"ip"`
	Port uint16 `json:"port"`
}

type dhtCompactNode struct {
	ID   protocol.ID    `json:"id"`
	Addr dhtCompactAddr `json:"addr"`
}

type dhtMessageArgs struct {
	ID          protocol.ID  `json:"id"`
	InfoHash    *protocol.ID `json:"infoHash,omitempty"`
	Target      *protocol.ID `json:"target,omitempty"`
	Token       dhtBytes     `json:"token,omitempty"`
	Port        *int64       `json:"port,omitempty"`
	ImpliedPort bool         `json:"impliedPort,omitempty"`
	Want        *[]dhtBytes  `json:"want,omitempty"`
	NoSeed      int64        `json:"noSeed,omitempty"`
	Scrape      int64        `json:"scrape,omitempty"`
}

type dhtMessageReturn struct {
	ID       protocol.ID       `json:"id"`
	Nodes    *[]dhtCompactNode `json:"nodes,omitempty"`
	Nodes6   *[]dhtCompactNode `json:"nodes6,omitempty"`
	Token    *dhtBytes         `json:"token,omitempty"`
	Values   *[]dhtCompactAddr `json:"values,omitempty"`
	Interval *int64            `json:"interval,omitempty"`
	Num      *int64            `json:"num,omitempty"`
	Samples  *[]protocol.ID    `json:"samples,omitempty"`
}

type dhtKRPCError struct {
	Code    int64    `json:"code"`
	Message dhtBytes `json:"message"`
}

type dhtKRPCMessage struct {
	TransactionID dhtBytes          `json:"transactionId"`
	MessageType   dhtBytes          `json:"messageType"`
	Query         dhtBytes          `json:"query,omitempty"`
	Args          *dhtMessageArgs   `json:"args,omitempty"`
	Response      *dhtMessageReturn `json:"response,omitempty"`
	Error         *dhtKRPCError     `json:"error,omitempty"`
	ObservedAddr  *dhtCompactAddr   `json:"observedAddr,omitempty"`
	ReadOnly      bool              `json:"readOnly,omitempty"`
	ClientID      dhtBytes          `json:"clientId,omitempty"`
}

type dhtKRPCWireExpected struct {
	WireHex            string         `json:"wireHex"`
	GoDecoded          dhtKRPCMessage `json:"goDecoded"`
	GoCanonicalWireHex string         `json:"goCanonicalWireHex"`
	RoundTripStable    bool           `json:"roundTripStable"`
}

type dhtKRPCCompatibilityInput struct {
	WireHex string `json:"wireHex"`
}

type dhtKRPCCompatibilityExpected struct {
	GoAccepted           bool   `json:"goAccepted"`
	RustAccepted         bool   `json:"rustAccepted"`
	GoCanonicalWireHex   string `json:"goCanonicalWireHex,omitempty"`
	RustCanonicalWireHex string `json:"rustCanonicalWireHex,omitempty"`
	Reason               string `json:"reason"`
}

func dhtID(fill byte) protocol.ID {
	var id protocol.ID
	for index := range id {
		id[index] = fill
	}
	return id
}

func i64ptr(value int64) *int64       { return &value }
func intptr(value int) *int           { return &value }
func stringptr(value string) *string  { return &value }
func bytesptr(value string) *dhtBytes { result := dhtBytes(value); return &result }

func dhtKRPCScenarios() []struct {
	id  string
	msg dht.Msg
} {
	return []struct {
		id  string
		msg dht.Msg
	}{
		{"empty_go_value", dht.Msg{}},
		{"ping_binary_transaction", dht.Msg{
			T: "\x00\xff", Y: dht.YQuery, Q: dht.QPing,
			A: &dht.MsgArgs{ID: dhtID(0x11)},
		}},
		{"find_node_ipv4_ipv6_want", dht.Msg{
			T: "fn", Y: dht.YQuery, Q: dht.QFindNode,
			A: &dht.MsgArgs{ID: dhtID(0x12), Target: dhtID(0x22), Want: []dht.Want{dht.WantNodes, dht.WantNodes6}},
		}},
		{"query_explicit_empty_want", dht.Msg{
			T: "we", Y: dht.YQuery, Q: dht.QFindNode,
			A: &dht.MsgArgs{ID: dhtID(0x12), Target: dhtID(0x23), Want: []dht.Want{}},
		}},
		{"get_peers_scrape", dht.Msg{
			T: "gp", Y: dht.YQuery, Q: dht.QGetPeers,
			A: &dht.MsgArgs{ID: dhtID(0x13), InfoHash: dhtID(0x33), Want: []dht.Want{dht.WantNodes6}, NoSeed: 1, Scrape: 1},
		}},
		{"announce_peer", dht.Msg{
			T: "ap", Y: dht.YQuery, Q: dht.QAnnouncePeer,
			A: &dht.MsgArgs{ID: dhtID(0x14), InfoHash: dhtID(0x44), Token: "\x00token\xff", Port: intptr(0xbeef)},
		}},
		{"announce_peer_implied", dht.Msg{
			T: "ai", Y: dht.YQuery, Q: dht.QAnnouncePeer,
			A: &dht.MsgArgs{ID: dhtID(0x15), InfoHash: dhtID(0x45), Token: "token", ImpliedPort: true},
		}},
		{"response_nodes_peers_and_empty_token", dht.Msg{
			T: "rs", Y: dht.YResponse,
			R: &dht.Return{
				ID:     dhtID(0x16),
				Nodes:  dht.CompactIPv4NodeInfo{{ID: dhtID(0x51), Addr: dht.NodeAddr{IP: net.ParseIP("1.2.3.4").To4(), Port: 0x1234}}},
				Nodes6: dht.CompactIPv6NodeInfo{{ID: dhtID(0x61), Addr: dht.NodeAddr{IP: net.ParseIP("2001:db8::1").To16(), Port: 0x5678}}},
				Token:  stringptr(""),
				Values: []dht.NodeAddr{
					{IP: net.ParseIP("5.6.7.8").To4(), Port: 0x9abc},
					{IP: net.ParseIP("2001:db8::2").To16(), Port: 0xdef0},
				},
			},
		}},
		{"response_explicit_empty_collections", dht.Msg{
			T: "re", Y: dht.YResponse,
			R: &dht.Return{
				ID: dhtID(0x16), Nodes: dht.CompactIPv4NodeInfo{},
				Nodes6: dht.CompactIPv6NodeInfo{}, Values: []dht.NodeAddr{},
			},
		}},
		{"sample_infohashes_explicit_empty", dht.Msg{
			T: "se", Y: dht.YResponse,
			R: &dht.Return{ID: dhtID(0x17), Bep51Return: dht.Bep51Return{Interval: i64ptr(420), Num: i64ptr(0), Samples: &dht.CompactInfohashes{}}},
		}},
		{"sample_infohashes_nonempty", dht.Msg{
			T: "sn", Y: dht.YResponse,
			R: &dht.Return{ID: dhtID(0x18), Bep51Return: dht.Bep51Return{Interval: i64ptr(300), Num: i64ptr(2), Samples: &dht.CompactInfohashes{dhtID(0x71), dhtID(0x72)}}},
		}},
		{"binary_error", dht.Msg{
			T: "\xfe\x00", Y: dht.YError, E: &dht.Error{Code: 203, Msg: "bad\x00\xff"},
		}},
		{"observed_addr_read_only_client", dht.Msg{
			T: "meta", Y: dht.YResponse, R: &dht.Return{ID: dhtID(0x19)},
			IP: dht.NodeAddr{IP: net.ParseIP("203.0.113.9").To4(), Port: 6881}, ReadOnly: true, ClientID: "\x00BM\xff",
		}},
	}
}

func projectDHTMessage(msg dht.Msg) dhtKRPCMessage {
	result := dhtKRPCMessage{
		TransactionID: dhtBytes(msg.T), MessageType: dhtBytes(msg.Y), ReadOnly: msg.ReadOnly,
	}
	if msg.Q != "" {
		result.Query = dhtBytes(msg.Q)
	}
	if msg.ClientID != "" {
		result.ClientID = dhtBytes(msg.ClientID)
	}
	if msg.IP.IP != nil {
		addr := projectDHTAddr(msg.IP)
		result.ObservedAddr = &addr
	}
	if msg.A != nil {
		args := dhtMessageArgs{ID: msg.A.ID, ImpliedPort: msg.A.ImpliedPort, NoSeed: int64(msg.A.NoSeed), Scrape: int64(msg.A.Scrape)}
		if !msg.A.InfoHash.IsZero() {
			value := msg.A.InfoHash
			args.InfoHash = &value
		}
		if !msg.A.Target.IsZero() {
			value := msg.A.Target
			args.Target = &value
		}
		if msg.A.Token != "" {
			args.Token = dhtBytes(msg.A.Token)
		}
		if msg.A.Port != nil {
			value := int64(*msg.A.Port)
			args.Port = &value
		}
		if msg.A.Want != nil {
			want := make([]dhtBytes, len(msg.A.Want))
			for index, value := range msg.A.Want {
				want[index] = dhtBytes(value)
			}
			args.Want = &want
		}
		result.Args = &args
	}
	if msg.R != nil {
		response := dhtMessageReturn{ID: msg.R.ID, Interval: msg.R.Interval, Num: msg.R.Num}
		if msg.R.Nodes != nil {
			nodes := make([]dhtCompactNode, len(msg.R.Nodes))
			for index, node := range msg.R.Nodes {
				nodes[index] = projectDHTNode(node)
			}
			response.Nodes = &nodes
		}
		if msg.R.Nodes6 != nil {
			nodes6 := make([]dhtCompactNode, len(msg.R.Nodes6))
			for index, node := range msg.R.Nodes6 {
				nodes6[index] = projectDHTNode(node)
			}
			response.Nodes6 = &nodes6
		}
		if msg.R.Token != nil {
			response.Token = bytesptr(*msg.R.Token)
		}
		if msg.R.Values != nil {
			values := make([]dhtCompactAddr, len(msg.R.Values))
			for index, addr := range msg.R.Values {
				values[index] = projectDHTAddr(addr)
			}
			response.Values = &values
		}
		if msg.R.Samples != nil {
			values := make([]protocol.ID, len(*msg.R.Samples))
			copy(values, *msg.R.Samples)
			response.Samples = &values
		}
		result.Response = &response
	}
	if msg.E != nil {
		result.Error = &dhtKRPCError{Code: int64(msg.E.Code), Message: dhtBytes(msg.E.Msg)}
	}
	return result
}

func projectDHTAddr(addr dht.NodeAddr) dhtCompactAddr {
	return dhtCompactAddr{IP: addr.IP.String(), Port: uint16(addr.Port)}
}

func projectDHTNode(node dht.NodeInfo) dhtCompactNode {
	return dhtCompactNode{ID: node.ID, Addr: projectDHTAddr(node.Addr)}
}

func TestGenerateDHTKRPCWireFixtures(t *testing.T) {
	fixtures := make([]Fixture, 0, len(dhtKRPCScenarios())+13)
	for _, scenario := range dhtKRPCScenarios() {
		wire, err := bencode.Marshal(scenario.msg)
		if err != nil {
			t.Fatalf("%s: Go encode: %v", scenario.id, err)
		}
		var decoded dht.Msg
		if err := bencode.Unmarshal(wire, &decoded); err != nil {
			t.Fatalf("%s: Go decode: %v", scenario.id, err)
		}
		canonical := mustBencode(t, decoded)
		fixtures = append(fixtures, mustFixture(
			t,
			scenario.id,
			projectDHTMessage(scenario.msg),
			dhtKRPCWireExpected{
				WireHex:            hex.EncodeToString(wire),
				GoDecoded:          projectDHTMessage(decoded),
				GoCanonicalWireHex: hex.EncodeToString(canonical),
				RoundTripStable:    bytes.Equal(wire, canonical),
			},
		))
	}

	compatibility := []struct {
		id, wire, reason string
		rustAccepted     bool
		rustCanonical    string
	}{
		{"missing_t_y", "de", "Go zero-values missing t/y; Rust preserves the projection and canonically emits them", true, "d1:t0:1:y0:e"},
		{"read_only_zero", "d2:roi0e1:t0:1:y0:e", "Go decodes explicit zero as false; Rust matches and omits it on canonical encode", true, "d1:t0:1:y0:e"},
		{"read_only_string", "d2:ro4:true1:t0:1:y0:e", "Go accepts a byte string for bool; Rust matches and emits canonical integer true", true, "d2:roi1e1:t0:1:y0:e"},
		{"implied_port_singleton_string", "d1:ad2:id20:0000000000000000000012:implied_portl4:trueee1:t0:1:y1:qe", "Go unwraps a singleton list and accepts a byte string bool; Rust matches and emits canonical integer true", true, "d1:ad2:id20:0000000000000000000012:implied_porti1ee1:t0:1:y1:qe"},
		{"unknown_top_level", "d1:t0:1:y0:1:z1:xe", "Go ignores unknown keys; Rust matches and omits them on canonical encode", true, "d1:t0:1:y0:e"},
		{"legacy_bare_error", "d1:e4:oops1:t0:1:y1:ee", "Go accepts the legacy bare error string; Rust matches and emits the canonical list", true, "d1:eli0e4:oopse1:t0:1:y1:ee"},
		{"zero_optional_ids", "d1:ad2:id20:111111111111111111119:info_hash20:" + string(make([]byte, 20)) + "6:target20:" + string(make([]byte, 20)) + "e1:t0:1:y1:qe", "Go collapses explicit all-zero optional fixed arrays; Rust matches and omits them canonically", true, "d1:ad2:id20:11111111111111111111e1:t0:1:y1:qe"},
		{"short_id", "d1:ad2:id1:xe1:t0:1:y1:qe", "Go's custom ID decoder and Rust both require exactly 20 bytes", false, ""},
		{"short_peer_addr", "d1:rd2:id20:000000000000000000006:valuesl3:abcee1:t0:1:y1:re", "Go accepts an arbitrary IP width; Rust deliberately requires BEP compact IPv4 or IPv6 width", false, ""},
		{"unsorted_keys", "d1:y0:1:t0:e", "Go accepts unsorted dictionaries; bendy deliberately rejects noncanonical syntax", false, ""},
		{"duplicate_keys", "d1:t0:1:t0:1:y0:e", "Go acceptance is probed; bendy deliberately rejects duplicate keys", false, ""},
		{"noncanonical_integer", "d2:roi00e1:t0:1:y0:e", "Go acceptance is probed; bendy deliberately rejects noncanonical integers", false, ""},
		{"trailing_value", "d1:t0:1:y0:e0:", "Both decoders must reject a trailing bencode value", false, ""},
	}
	for _, scenario := range compatibility {
		wire := []byte(scenario.wire)
		var decoded dht.Msg
		err := bencode.Unmarshal(wire, &decoded)
		expected := dhtKRPCCompatibilityExpected{GoAccepted: err == nil, RustAccepted: scenario.rustAccepted, RustCanonicalWireHex: hex.EncodeToString([]byte(scenario.rustCanonical)), Reason: scenario.reason}
		if err == nil {
			expected.GoCanonicalWireHex = hex.EncodeToString(mustBencode(t, decoded))
		}
		fixtures = append(fixtures, mustFixture(t, "compat_"+scenario.id, dhtKRPCCompatibilityInput{WireHex: hex.EncodeToString(wire)}, expected))
	}
	reconcileDHTFixtures(t, "krpc_wire.jsonl", fixtures)
}

func mustBencode(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := bencode.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func mustFixture(t *testing.T, id string, input, expected any) Fixture {
	t.Helper()
	inputJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	return Fixture{ID: id, Subsystem: dhtKRPCWireSubsystem, Input: inputJSON, Expected: expectedJSON}
}

func reconcileDHTFixtures(t *testing.T, filename string, fixtures []Fixture) {
	t.Helper()
	var output bytes.Buffer
	for _, fixture := range fixtures {
		encoded, err := json.Marshal(fixture)
		if err != nil {
			t.Fatal(err)
		}
		output.Write(encoded)
		output.WriteByte('\n')
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve DHT fixture source")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(source), "..", "..", "testdata", "parity", "dht", filename))
	if *updateQueueParity {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, output.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read DHT golden (run -update-queue-parity): %v", err)
	}
	if !bytes.Equal(expected, output.Bytes()) {
		line, want, got := firstJSONLDifference(expected, output.Bytes())
		t.Fatalf("DHT golden differs line %d\nwant: %s\n got: %s", line, want, got)
	}
}
