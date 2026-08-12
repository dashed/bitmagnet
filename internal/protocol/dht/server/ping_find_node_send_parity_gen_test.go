package server

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
)

var updateDHTPingFindNodeSendParity = flag.Bool(
	"update-dht-ping-find-node-send-parity",
	false,
	"rewrite the Rust DHT ping/find-node send parity fixture",
)

type pingFindNodeSendFixture struct {
	ID        string                   `json:"id"`
	Subsystem string                   `json:"subsystem"`
	Input     pingFindNodeSendInput    `json:"input"`
	Expected  pingFindNodeSendExpected `json:"expected"`
}

type pingFindNodeSendInput struct {
	Destination   pingFindNodeSendAddr   `json:"destination"`
	TIDHex        string                 `json:"tidHex"`
	Kind          string                 `json:"kind"`
	NodeAddrs     []pingFindNodeSendAddr `json:"nodeAddrs,omitempty"`
	TransportFail bool                   `json:"transportFail,omitempty"`
}

type pingFindNodeSendAddr struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope,omitempty"`
}

type pingFindNodeSendExpected struct {
	WireHex            string `json:"wireHex,omitempty"`
	SendCalls          int    `json:"sendCalls"`
	GoPanicked         bool   `json:"goPanicked,omitempty"`
	TransportErrorSame bool   `json:"transportErrorSame,omitempty"`
}

type pingFindNodeSendSocket struct {
	destinations []netip.AddrPort
	wires        [][]byte
	err          error
}

func (*pingFindNodeSendSocket) Open(netip.AddrPort) error { return nil }
func (*pingFindNodeSendSocket) Close() error              { return nil }
func (s *pingFindNodeSendSocket) Send(destination netip.AddrPort, wire []byte) error {
	s.destinations = append(s.destinations, destination)
	s.wires = append(s.wires, append([]byte(nil), wire...))
	return s.err
}
func (*pingFindNodeSendSocket) Receive([]byte) (int, netip.AddrPort, error) {
	return 0, netip.AddrPort{}, errors.New("receive is outside the send oracle")
}

func TestGenerateDHTPingFindNodeSendParity(t *testing.T) {
	scenarios := []pingFindNodeSendInput{
		{
			Destination: pingFindNodeSendAddr{IP: "192.0.2.1", Port: 0},
			TIDHex:      "",
			Kind:        "success",
		},
		{
			Destination: pingFindNodeSendAddr{IP: "::ffff:192.0.2.2", Port: 6881},
			TIDHex:      "ff",
			Kind:        "error",
		},
		{
			Destination: pingFindNodeSendAddr{IP: "fe80::3", Port: 1, Scope: 7},
			TIDHex:      "000102",
			Kind:        "success",
		},
		{
			Destination:   pingFindNodeSendAddr{IP: "2001:db8::4", Port: 2},
			TIDHex:        "00ff",
			Kind:          "success",
			TransportFail: true,
		},
		{
			Destination: pingFindNodeSendAddr{IP: "192.0.2.5", Port: 3},
			TIDHex:      "4e31",
			Kind:        "success",
			NodeAddrs: []pingFindNodeSendAddr{{
				IP: "2001:db8::5", Port: 6881,
			}},
		},
		{
			Destination: pingFindNodeSendAddr{IP: "192.0.2.6", Port: 4},
			TIDHex:      "4d31",
			Kind:        "success",
			NodeAddrs: []pingFindNodeSendAddr{
				{IP: "192.0.2.60", Port: 60},
				{IP: "2001:db8::6", Port: 61},
			},
		},
	}
	ids := []string{
		"success_empty_tid_ipv4",
		"error_one_byte_tid_mapped_destination",
		"success_three_byte_tid_scoped_destination",
		"transport_error_binary_tid",
		"native_ipv6_encode_panics_before_socket",
		"mixed_nodes_encode_panics_before_socket",
	}

	fixtures := make([]pingFindNodeSendFixture, 0, len(scenarios))
	for index, scenario := range scenarios {
		fixtures = append(fixtures, runPingFindNodeSendScenario(t, ids[index], scenario))
	}
	reconcilePingFindNodeSendFixtures(t, fixtures)
}

func runPingFindNodeSendScenario(t *testing.T, id string, input pingFindNodeSendInput) pingFindNodeSendFixture {
	t.Helper()
	sentinel := errors.New("oracle transport sentinel")
	socket := &pingFindNodeSendSocket{}
	if input.TransportFail {
		socket.err = sentinel
	}
	message := pingFindNodeSendMessage(t, input)
	server := &server{socket: socket}
	err, panicked := callPingFindNodeServerSend(server, input.Destination.addrPort(), message)
	expected := pingFindNodeSendExpected{
		SendCalls:          len(socket.wires),
		GoPanicked:         panicked,
		TransportErrorSame: input.TransportFail && err == sentinel,
	}
	if len(socket.wires) == 1 {
		expected.WireHex = hex.EncodeToString(socket.wires[0])
		if socket.destinations[0] != input.Destination.addrPort() {
			t.Fatalf("%s: destination changed to %s", id, socket.destinations[0])
		}
	}
	if input.TransportFail {
		if panicked || !expected.TransportErrorSame || expected.SendCalls != 1 {
			t.Fatalf("%s: transport error was not returned unchanged after one call", id)
		}
	} else if len(input.NodeAddrs) != 0 && hasNativeSendAddr(input.NodeAddrs) {
		if !panicked || err != nil || expected.SendCalls != 0 {
			t.Fatalf("%s: invalid compact IPv4 nodes did not panic before Send", id)
		}
	} else if panicked || err != nil || expected.SendCalls != 1 {
		t.Fatalf("%s: successful send result panic=%v err=%v calls=%d", id, panicked, err, expected.SendCalls)
	}
	return pingFindNodeSendFixture{
		ID:        id,
		Subsystem: "dht_ping_find_node_send",
		Input:     input,
		Expected:  expected,
	}
}

func callPingFindNodeServerSend(server *server, destination netip.AddrPort, message dht.Msg) (err error, panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	err = server.send(destination, message)
	return
}

func pingFindNodeSendMessage(t *testing.T, input pingFindNodeSendInput) dht.Msg {
	t.Helper()
	tid, err := hex.DecodeString(input.TIDHex)
	if err != nil {
		t.Fatal(err)
	}
	message := dht.Msg{T: string(tid), Y: dht.YResponse}
	switch input.Kind {
	case "success":
		ret := dht.Return{ID: pingFindNodeSendID(0x90)}
		for index, addr := range input.NodeAddrs {
			ret.Nodes = append(ret.Nodes, dht.NodeInfo{
				ID:   pingFindNodeSendID(byte(index + 1)),
				Addr: dht.NewNodeAddrFromAddrPort(addr.addrPort()),
			})
		}
		message.R = &ret
	case "error":
		message.E = &dht.Error{Code: dht.ErrorCodeProtocolError, Msg: "missing arguments"}
	default:
		t.Fatalf("unknown message kind %q", input.Kind)
	}
	return message
}

func pingFindNodeSendID(last byte) protocol.ID {
	var id protocol.ID
	id[19] = last
	return id
}

func (addr pingFindNodeSendAddr) addrPort() netip.AddrPort {
	ip := netip.MustParseAddr(addr.IP)
	if addr.Scope != 0 {
		ip = ip.WithZone(strconv.FormatUint(uint64(addr.Scope), 10))
	}
	return netip.AddrPortFrom(ip, addr.Port)
}

func hasNativeSendAddr(addrs []pingFindNodeSendAddr) bool {
	for _, addr := range addrs {
		if ip := netip.MustParseAddr(addr.IP); ip.Is6() && !ip.Is4In6() {
			return true
		}
	}
	return false
}

func reconcilePingFindNodeSendFixtures(t *testing.T, fixtures []pingFindNodeSendFixture) {
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
	path := filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../testdata/parity/dht/ping_find_node_send.jsonl"))
	if *updateDHTPingFindNodeSendParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-ping-find-node-send-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("ping/find-node send fixture is stale; rerun with -update-dht-ping-find-node-send-parity")
	}
}
