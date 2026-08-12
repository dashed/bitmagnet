//go:build !windows

package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"golang.org/x/sys/unix"
)

var updateDHTTokioIPv4UDPParity = flag.Bool(
	"update-dht-tokio-ipv4-udp-parity",
	false,
	"rewrite the Rust DHT Tokio IPv4 UDP fixture",
)

type tokioIPv4UDPFixture struct {
	ID        string               `json:"id"`
	Subsystem string               `json:"subsystem"`
	Input     tokioIPv4UDPInput    `json:"input"`
	Expected  tokioIPv4UDPExpected `json:"expected"`
}

type tokioIPv4UDPInput struct {
	PayloadHex    string `json:"payloadHex,omitempty"`
	PayloadLength int    `json:"payloadLength"`
}

type tokioIPv4UDPExpected struct {
	Sent               bool   `json:"sent"`
	Received           bool   `json:"received"`
	Length             int    `json:"length"`
	SHA256Hex          string `json:"sha256Hex"`
	SourceIPv4         bool   `json:"sourceIPv4"`
	SourcePortNonzero  bool   `json:"sourcePortNonzero"`
	DestinationIPv4    bool   `json:"destinationIPv4"`
	DestinationNonzero bool   `json:"destinationPortNonzero"`
}

func TestGenerateDHTTokioIPv4UDPParity(t *testing.T) {
	scenarios := []struct {
		id      string
		payload []byte
	}{
		{id: "zero_length", payload: []byte{}},
		{id: "binary", payload: []byte{0, 1, 2, 0x7f, 0x80, 0xfe, 0xff}},
		{id: "safe_8192", payload: deterministicTokioIPv4Payload(8192)},
	}
	fixtures := make([]tokioIPv4UDPFixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixtures = append(fixtures, runTokioIPv4UDPScenario(t, scenario.id, scenario.payload))
	}
	reconcileTokioIPv4UDPFixtures(t, fixtures)
}

func TestDHTProductionIPv4SocketRejectsIPv6Families(t *testing.T) {
	sender := newSocket().(*socket)
	t.Cleanup(func() { _ = sender.Close() })
	if err := sender.Open(netip.MustParseAddrPort("127.0.0.1:0")); err != nil {
		t.Fatal(err)
	}
	for _, destination := range []netip.AddrPort{
		netip.MustParseAddrPort("[::1]:6881"),
		netip.MustParseAddrPort("[::ffff:127.0.0.1]:6881"),
	} {
		if err := sender.Send(destination, []byte("family")); err == nil {
			t.Fatalf("AF_INET production socket unexpectedly accepted %s", destination)
		}
	}
}

func runTokioIPv4UDPScenario(t *testing.T, id string, payload []byte) tokioIPv4UDPFixture {
	t.Helper()
	receiver := newSocket().(*socket)
	sender := newSocket().(*socket)
	t.Cleanup(func() {
		_ = receiver.Close()
		_ = sender.Close()
	})
	if err := receiver.Open(netip.MustParseAddrPort("127.0.0.1:0")); err != nil {
		t.Fatal(err)
	}
	if err := sender.Open(netip.MustParseAddrPort("127.0.0.1:0")); err != nil {
		t.Fatal(err)
	}
	receiverAddr := tokioIPv4UDPSockname(t, receiver.fd)
	senderAddr := tokioIPv4UDPSockname(t, sender.fd)
	if err := sender.Send(receiverAddr, payload); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 65507)
	n, source, err := receiver.Receive(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buffer[:n], payload) {
		t.Fatal("actual production socket changed payload bytes")
	}
	if source != senderAddr {
		t.Fatalf("actual production socket source = %s, want %s", source, senderAddr)
	}
	digest := sha256.Sum256(buffer[:n])
	return tokioIPv4UDPFixture{
		ID: id, Subsystem: "dht_tokio_ipv4_udp",
		Input: tokioIPv4UDPInput{
			PayloadHex: hex.EncodeToString(payload), PayloadLength: len(payload),
		},
		Expected: tokioIPv4UDPExpected{
			Sent: true, Received: true, Length: n, SHA256Hex: hex.EncodeToString(digest[:]),
			SourceIPv4: source.Addr().Is4(), SourcePortNonzero: source.Port() != 0,
			DestinationIPv4: receiverAddr.Addr().Is4(), DestinationNonzero: receiverAddr.Port() != 0,
		},
	}
}

func tokioIPv4UDPSockname(t *testing.T, fd int) netip.AddrPort {
	t.Helper()
	sockaddr, err := unix.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := sockaddrToAddrPort(sockaddr)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func deterministicTokioIPv4Payload(length int) []byte {
	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	return payload
}

func reconcileTokioIPv4UDPFixtures(t *testing.T, fixtures []tokioIPv4UDPFixture) {
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
		filepath.Dir(source),
		"../../../../testdata/parity/dht/tokio_ipv4_udp.jsonl",
	))
	if *updateDHTTokioIPv4UDPParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-tokio-ipv4-udp-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("Tokio IPv4 UDP fixture is stale; rerun with -update-dht-tokio-ipv4-udp-parity")
	}
}
