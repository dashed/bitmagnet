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
)

var updateDHTQuerySendParity = flag.Bool(
	"update-dht-query-send-parity",
	false,
	"rewrite the Rust DHT registered query-send fixture",
)

type querySendParityFixture struct {
	ID        string                  `json:"id"`
	Subsystem string                  `json:"subsystem"`
	Input     querySendParityInput    `json:"input"`
	Expected  querySendParityExpected `json:"expected"`
}

type querySendParityInput struct {
	IssuerTIDsHex  []string            `json:"issuerTidsHex"`
	PreexistingTID string              `json:"preexistingTidHex,omitempty"`
	Remote         querySendParityAddr `json:"remote"`
	QueryHex       string              `json:"queryHex"`
	LocalID        string              `json:"localId"`
	Target         *string             `json:"target,omitempty"`
	DeliverDuring  bool                `json:"deliverDuringSend,omitempty"`
	FailSend       bool                `json:"failSend,omitempty"`
}

type querySendParityExpected struct {
	TIDHex                 string              `json:"tidHex"`
	WireHex                string              `json:"wireHex"`
	Destination            querySendParityAddr `json:"destination"`
	RegisteredAtSend       bool                `json:"registeredAtSend"`
	DeliveryBuffered       bool                `json:"deliveryBuffered,omitempty"`
	SendCalls              int                 `json:"sendCalls"`
	IssuerCalls            int                 `json:"issuerCalls"`
	Outcome                string              `json:"outcome"`
	ResponseID             string              `json:"responseId,omitempty"`
	TransportErrorIdentity bool                `json:"transportErrorIdentity,omitempty"`
	OwnedPendingAfter      bool                `json:"ownedPendingAfter"`
	TotalPendingAfter      int                 `json:"totalPendingAfter"`
	Events                 []string            `json:"events"`
}

type querySendParityAddr struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope,omitempty"`
}

type querySendParityScenario struct {
	id                string
	issuerTIDs        []string
	preexistingTID    string
	remote            netip.AddrPort
	query             string
	args              dht.MsgArgs
	deliverDuringSend bool
	failSend          bool
	responseID        protocol.ID
}

type querySendParityIssuer struct {
	mu    sync.Mutex
	ids   []string
	calls int
}

func (i *querySendParityIssuer) Issue() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	id := i.ids[i.calls]
	i.calls++
	return id
}

type querySendParitySocket struct {
	server            *server
	deliverDuringSend bool
	responseID        protocol.ID
	sendErr           error
	sends             []querySendParitySend
	events            []string
}

type querySendParitySend struct {
	tid              string
	destination      netip.AddrPort
	wireHex          string
	registeredAtSend bool
	deliveryBuffered bool
}

func (*querySendParitySocket) Open(netip.AddrPort) error { return nil }
func (*querySendParitySocket) Close() error              { return nil }
func (*querySendParitySocket) Receive([]byte) (int, netip.AddrPort, error) {
	return 0, netip.AddrPort{}, context.Canceled
}

func (s *querySendParitySocket) Send(addr netip.AddrPort, wire []byte) error {
	var message dht.Msg
	if err := bencode.Unmarshal(wire, &message); err != nil {
		return err
	}
	s.server.mutex.Lock()
	pending, registered := s.server.queries[message.T]
	registered = registered && pending.addr == addr
	s.server.mutex.Unlock()
	s.sends = append(s.sends, querySendParitySend{
		tid: hex.EncodeToString([]byte(message.T)), destination: addr,
		wireHex: hex.EncodeToString(wire), registeredAtSend: registered,
	})
	s.events = append(s.events, "send")
	if s.deliverDuringSend {
		s.server.handleResponse(dht.RecvMsg{
			From: addr,
			Msg: dht.Msg{
				T: message.T,
				Y: dht.YResponse,
				R: &dht.Return{ID: s.responseID},
			},
		})
		s.sends[len(s.sends)-1].deliveryBuffered = len(pending.ch) == 1
		s.events = append(s.events, "deliver")
	}
	return s.sendErr
}

func TestGenerateDHTQuerySendParity(t *testing.T) {
	localID := querySendParityID(0x11)
	target := querySendParityID(0x22)
	responseID := querySendParityID(0x33)
	scenarios := []querySendParityScenario{
		{
			id: "ping_ipv4_response_during_send", issuerTIDs: []string{"P1"},
			remote: netip.MustParseAddrPort("192.0.2.1:6881"), query: dht.QPing,
			args: dht.MsgArgs{ID: localID}, deliverDuringSend: true, responseID: responseID,
		},
		{
			id: "find_node_mapped_response_during_send", issuerTIDs: []string{"F1"},
			remote: netip.MustParseAddrPort("[::ffff:192.0.2.2]:6882"), query: dht.QFindNode,
			args: dht.MsgArgs{ID: localID, Target: target}, deliverDuringSend: true, responseID: responseID,
		},
		{
			id: "binary_query_scoped_ipv6", issuerTIDs: []string{"B1"},
			remote: netip.MustParseAddrPort("[fe80::3%7]:6883"), query: string([]byte{0, 255}),
			args: dht.MsgArgs{ID: localID}, deliverDuringSend: true, responseID: responseID,
		},
		{
			id: "collision_retries_before_send", issuerTIDs: []string{"A1", "B2"},
			preexistingTID: "A1", remote: netip.MustParseAddrPort("192.0.2.4:6884"),
			query: dht.QPing, args: dht.MsgArgs{ID: localID}, deliverDuringSend: true,
			responseID: responseID,
		},
		{
			id: "transport_error", issuerTIDs: []string{"E1"},
			remote: netip.MustParseAddrPort("192.0.2.5:6885"), query: dht.QPing,
			args: dht.MsgArgs{ID: localID}, failSend: true,
		},
		{
			id: "transport_error_after_delivery_wins", issuerTIDs: []string{"E2"},
			remote: netip.MustParseAddrPort("[2001:db8::6]:6886"), query: dht.QFindNode,
			args: dht.MsgArgs{ID: localID, Target: target}, deliverDuringSend: true,
			failSend: true, responseID: responseID,
		},
	}

	fixtures := make([]querySendParityFixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixtures = append(fixtures, runQuerySendParityScenario(t, scenario))
	}
	reconcileQuerySendParityFixtures(t, fixtures)
}

func runQuerySendParityScenario(
	t *testing.T,
	scenario querySendParityScenario,
) querySendParityFixture {
	t.Helper()
	issuer := &querySendParityIssuer{ids: scenario.issuerTIDs}
	sentinel := errors.New("query send oracle sentinel")
	socket := &querySendParitySocket{
		deliverDuringSend: scenario.deliverDuringSend,
		responseID:        scenario.responseID,
	}
	if scenario.failSend {
		socket.sendErr = sentinel
	}
	s := &server{
		socket: socket, queryTimeout: time.Minute, queries: make(map[string]pendingQuery),
		idIssuer: issuer, logger: zap.NewNop().Sugar(),
	}
	socket.server = s
	if scenario.preexistingTID != "" {
		s.queries[scenario.preexistingTID] = pendingQuery{
			ch: make(chan dht.RecvMsg, 1), addr: netip.MustParseAddrPort("198.51.100.1:1"),
		}
	}
	result, err := s.Query(context.Background(), scenario.remote, scenario.query, scenario.args)
	socket.events = append(socket.events, "return")
	if len(socket.sends) != 1 || !socket.sends[0].registeredAtSend {
		t.Fatalf("%s: query was not registered inside its only Send", scenario.id)
	}
	if scenario.deliverDuringSend && !socket.sends[0].deliveryBuffered {
		t.Fatalf("%s: response was not buffered before Send returned", scenario.id)
	}
	tidBytes, decodeErr := hex.DecodeString(socket.sends[0].tid)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	tid := string(tidBytes)
	s.mutex.Lock()
	_, ownedPendingAfter := s.queries[tid]
	totalPendingAfter := len(s.queries)
	s.mutex.Unlock()
	outcome := "response"
	if scenario.failSend {
		outcome = "transport_error"
		if err != sentinel {
			t.Fatalf("%s: transport sentinel identity changed", scenario.id)
		}
	} else if err != nil {
		t.Fatalf("%s: unexpected query error: %v", scenario.id, err)
	}

	input := querySendParityInput{
		IssuerTIDsHex: make([]string, 0, len(scenario.issuerTIDs)),
		Remote:        querySendParityProjectAddr(scenario.remote), QueryHex: hex.EncodeToString([]byte(scenario.query)),
		LocalID: scenario.args.ID.String(), DeliverDuring: scenario.deliverDuringSend,
		FailSend: scenario.failSend,
	}
	for _, value := range scenario.issuerTIDs {
		input.IssuerTIDsHex = append(input.IssuerTIDsHex, hex.EncodeToString([]byte(value)))
	}
	if scenario.preexistingTID != "" {
		input.PreexistingTID = hex.EncodeToString([]byte(scenario.preexistingTID))
	}
	if !scenario.args.Target.IsZero() {
		value := scenario.args.Target.String()
		input.Target = &value
	}
	expected := querySendParityExpected{
		TIDHex: socket.sends[0].tid, WireHex: socket.sends[0].wireHex,
		Destination:      querySendParityProjectAddr(socket.sends[0].destination),
		RegisteredAtSend: socket.sends[0].registeredAtSend,
		DeliveryBuffered: socket.sends[0].deliveryBuffered, SendCalls: len(socket.sends),
		IssuerCalls: issuer.calls, Outcome: outcome,
		TransportErrorIdentity: scenario.failSend && err == sentinel,
		OwnedPendingAfter:      ownedPendingAfter, TotalPendingAfter: totalPendingAfter,
		Events: append([]string(nil), socket.events...),
	}
	if err == nil {
		expected.ResponseID = result.Msg.R.ID.String()
	}
	return querySendParityFixture{
		ID: scenario.id, Subsystem: "dht_query_send", Input: input, Expected: expected,
	}
}

func querySendParityID(last byte) protocol.ID {
	var id protocol.ID
	id[19] = last
	return id
}

func querySendParityProjectAddr(addr netip.AddrPort) querySendParityAddr {
	scope := uint32(0)
	if addr.Addr().Zone() != "" {
		parsed, err := strconv.ParseUint(addr.Addr().Zone(), 10, 32)
		if err != nil {
			panic(err)
		}
		scope = uint32(parsed)
	}
	return querySendParityAddr{
		IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: scope,
	}
}

func reconcileQuerySendParityFixtures(t *testing.T, fixtures []querySendParityFixture) {
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
		filepath.Dir(source), "../../../../testdata/parity/dht/query_send.jsonl",
	))
	if *updateDHTQuerySendParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-query-send-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("query-send fixture is stale; rerun with -update-dht-query-send-parity")
	}
}
