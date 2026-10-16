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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable/btree"
	dhtresponder "github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/responder"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var updateDHTRuntimeBridgeParity = flag.Bool(
	"update-dht-runtime-bridge-parity",
	false,
	"rewrite the Rust DHT full receive/responder/send bridge fixture",
)

const (
	dhtRuntimeBridgeSubsystem = "dht_runtime_bridge"
	dhtRuntimeBridgeNodeID    = "00112233445566778899aabbccddeeff10203040"
	dhtRuntimeBridgeSecret    = "303132333435363738396162636465666768696a"
	dhtRuntimeBridgeRequester = "ffeeddccbbaa0099887766554433221100abcdef"
	dhtRuntimeBridgeInfoHash  = "11223344556677889900aabbccddeeff01020304"
	dhtRuntimeBridgeTarget    = "0000000000000000000000000000000000000011"
	dhtRuntimeBridgeToken     = "266127f80b327ff927362ec21a79e923"
	dhtRuntimeBridgeInterval  = int64(10)
)

var dhtRuntimeBridgeFixtureIDs = [...]string{
	"ping_success_empty_tid_mixed_fields",
	"find_node_populated_mapped_source",
	"get_peers_found_values_token",
	"get_peers_miss_nodes_token",
	"announce_peer_valid_mutates_before_send",
	"sample_infohashes_populated_scoped_source",
	"unknown_method_204",
	"missing_args_precedes_unknown_203",
	"unsorted_duplicate_query_decode",
	"announce_send_transport_failure_mutation_survives",
	"receive_transport_error_panics",
	"overreported_length_panics",
}

type dhtRuntimeBridgeWireGolden struct {
	inputHex    string
	responseHex string
}

// These wire bytes are deliberately independent of the production bencode
// encoder and the generated JSONL. The update flag cannot silently bless an
// encoder/order drift in either the injected query or the emitted response.
var dhtRuntimeBridgeWireGoldens = map[string]dhtRuntimeBridgeWireGolden{
	"ping_success_empty_tid_mixed_fields": {
		inputHex:    "64313a6164323a696432303affeeddccbbaa0099887766554433221100abcdef65313a656c693939396531323a726571756573742d6f6e6c7965313a71343a70696e67313a7264323a696432303a00112233445566778899aabbccddeeff1020304065323a726f693165313a74303a313a76323aff00313a79313a7165",
		responseHex: "64313a7264323a696432303a00112233445566778899aabbccddeeff1020304065313a74303a313a79313a7265",
	},
	"find_node_populated_mapped_source": {
		inputHex:    "64313a6164323a696432303affeeddccbbaa0099887766554433221100abcdef363a74617267657432303a000000000000000000000000000000000000001165313a71393a66696e645f6e6f6465313a74333a464e31313a79313a7165",
		responseHex: "64313a7264323a696432303a00112233445566778899aabbccddeeff10203040353a6e6f64657335323a0000000000000000000000000000000000000011c000020b132f0000000000000000000000000000000000000012c633640cffff65313a74333a464e31313a79313a7265",
	},
	"get_peers_found_values_token": {
		inputHex:    "64313a6164323a696432303affeeddccbbaa0099887766554433221100abcdef393a696e666f5f6861736832303a11223344556677889900aabbccddeeff0102030465313a71393a6765745f7065657273313a74333a475031313a79313a7165",
		responseHex: "64313a7264323a696432303a00112233445566778899aabbccddeeff10203040353a746f6b656e33323a3236363132376638306233323766663932373336326563323161373965393233363a76616c7565736c363acb0071150001363acb007116ffff6565313a74333a475031313a79313a7265",
	},
	"get_peers_miss_nodes_token": {
		inputHex:    "64313a6164323a696432303affeeddccbbaa0099887766554433221100abcdef393a696e666f5f6861736832303a11223344556677889900aabbccddeeff0102030465313a71393a6765745f7065657273313a74333a475032313a79313a7165",
		responseHex: "64313a7264323a696432303a00112233445566778899aabbccddeeff10203040353a6e6f64657335323a0000000000000000000000000000000000000011c000020b132f0000000000000000000000000000000000000012c633640cffff353a746f6b656e33323a323636313237663830623332376666393237333632656332316137396539323365313a74333a475032313a79313a7265",
	},
	"announce_peer_valid_mutates_before_send": {
		inputHex:    "64313a6164323a696432303affeeddccbbaa0099887766554433221100abcdef393a696e666f5f6861736832303a11223344556677889900aabbccddeeff01020304343a706f727469353134313365353a746f6b656e33323a323636313237663830623332376666393237333632656332316137396539323365313a7131333a616e6e6f756e63655f70656572313a74333a415031313a79313a7165",
		responseHex: "64313a7264323a696432303a00112233445566778899aabbccddeeff1020304065313a74333a415031313a79313a7265",
	},
	"sample_infohashes_populated_scoped_source": {
		inputHex:    "64313a6164323a696432303affeeddccbbaa0099887766554433221100abcdef363a74617267657432303a000000000000000000000000000000000000001165313a7131373a73616d706c655f696e666f686173686573313a74333a534931313a79313a7165",
		responseHex: "64313a7264323a696432303a00112233445566778899aabbccddeeff10203040383a696e74657276616c69313065353a6e6f64657335323a0000000000000000000000000000000000000012c633640cffff0000000000000000000000000000000000000011c000020b132f333a6e756d693265373a73616d706c657336303a00000000000000000000000000000000000000310000000000000000000000000000000000000031000000000000000000000000000000000000003265313a74333a534931313a79313a7265",
	},
	"unknown_method_204": {
		inputHex:    "64313a6164323a696432303affeeddccbbaa0099887766554433221100abcdef65313a71373a756e6b6e6f776e313a74323a5531313a79313a7165",
		responseHex: "64313a656c693230346531343a6d6574686f6420556e6b6e6f776e65313a74323a5531313a79313a7265",
	},
	"missing_args_precedes_unknown_203": {
		inputHex:    "64313a71373a756e6b6e6f776e313a74323a4d31313a79313a7165",
		responseHex: "64313a656c693230336531373a6d697373696e6720617267756d656e747365313a74323a4d31313a79313a7265",
	},
	"unsorted_duplicate_query_decode": {
		inputHex:    "64313a79313a71313a74323a4131313a74323a4132313a71343a70696e6765",
		responseHex: "64313a656c693230336531373a6d697373696e6720617267756d656e747365313a74323a4132313a79313a7265",
	},
	"announce_send_transport_failure_mutation_survives": {
		inputHex:    "64313a6164323a696432303affeeddccbbaa0099887766554433221100abcdef393a696e666f5f6861736832303a11223344556677889900aabbccddeeff01020304343a706f727469353134313365353a746f6b656e33323a323636313237663830623332376666393237333632656332316137396539323365313a7131333a616e6e6f756e63655f70656572313a74333a414631313a79313a7165",
		responseHex: "64313a7264323a696432303a00112233445566778899aabbccddeeff1020304065313a74333a414631313a79313a7265",
	},
	"receive_transport_error_panics": {},
	"overreported_length_panics":     {},
}

type dhtRuntimeBridgeFixture struct {
	ID        string                   `json:"id"`
	Subsystem string                   `json:"subsystem"`
	Runtime   dhtRuntimeBridgeRuntime  `json:"runtime"`
	Input     dhtRuntimeBridgeInput    `json:"input"`
	Expected  dhtRuntimeBridgeExpected `json:"expected"`
}

type dhtRuntimeBridgeRuntime struct {
	IntBits int `json:"intBits"`
}

type dhtRuntimeBridgeInput struct {
	WireHex string                       `json:"wireHex"`
	Source  dhtRuntimeBridgeAddr         `json:"source"`
	Config  dhtRuntimeBridgeConfig       `json:"config"`
	Table   dhtRuntimeBridgeTableScript  `json:"table"`
	Socket  dhtRuntimeBridgeSocketScript `json:"socket"`
}

type dhtRuntimeBridgeConfig struct {
	NodeID                   string `json:"nodeId"`
	TokenSecretHex           string `json:"tokenSecretHex"`
	SampleInfoHashesInterval int64  `json:"sampleInfoHashesInterval"`
}

type dhtRuntimeBridgeSocketScript struct {
	ReceiveKind    string `json:"receiveKind"`
	SendKind       string `json:"sendKind"`
	ReportedLength int    `json:"reportedLength"`
}

type dhtRuntimeBridgeExpected struct {
	Classification               string                        `json:"classification"`
	GoTerminal                   string                        `json:"goTerminal"`
	RustTerminal                 string                        `json:"rustTerminal"`
	ReceiveCalls                 int                           `json:"receiveCalls"`
	ResponderCalls               int                           `json:"responderCalls"`
	ResponderInputExact          bool                          `json:"responderInputExact"`
	SendCalls                    int                           `json:"sendCalls"`
	DestinationPresent           bool                          `json:"destinationPresent"`
	Destination                  dhtRuntimeBridgeAddr          `json:"destination"`
	WirePresent                  bool                          `json:"wirePresent"`
	WireHex                      string                        `json:"wireHex"`
	ContinuationReceiveEntered   bool                          `json:"continuationReceiveEntered"`
	SendAfterResponderReturn     bool                          `json:"sendAfterResponderReturn"`
	SendFailureLogged            bool                          `json:"sendFailureLogged"`
	SendFailureIdentityExact     bool                          `json:"sendFailureIdentityExact"`
	ReceivePanicRetainsTransport bool                          `json:"receivePanicRetainsTransport"`
	PanicClass                   string                        `json:"panicClass"`
	TableCalls                   []dhtRuntimeBridgeTableCall   `json:"tableCalls"`
	State                        dhtRuntimeBridgeExpectedState `json:"state"`
}

type dhtRuntimeBridgeExpectedState struct {
	Before []dhtRuntimeBridgePutHash `json:"before"`
	AtSend []dhtRuntimeBridgePutHash `json:"atSend"`
	After  []dhtRuntimeBridgePutHash `json:"after"`
}

type dhtRuntimeBridgeTableScript struct {
	ClosestNodes       []dhtRuntimeBridgeNode `json:"closestNodes"`
	LookupFound        bool                   `json:"lookupFound"`
	LookupHashID       string                 `json:"lookupHashId"`
	LookupPeers        []dhtRuntimeBridgeAddr `json:"lookupPeers"`
	LookupClosestNodes []dhtRuntimeBridgeNode `json:"lookupClosestNodes"`
	SampleHashes       []string               `json:"sampleHashes"`
	SampleNodes        []dhtRuntimeBridgeNode `json:"sampleNodes"`
	SampleTotalHashes  int64                  `json:"sampleTotalHashes"`
}

type dhtRuntimeBridgeAddr struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope"`
}

type dhtRuntimeBridgeNode struct {
	ID   string               `json:"id"`
	Addr dhtRuntimeBridgeAddr `json:"addr"`
}

type dhtRuntimeBridgeTableCall struct {
	Method       string `json:"method"`
	ID           string `json:"id"`
	CommandCount int    `json:"commandCount"`
}

type dhtRuntimeBridgePutHash struct {
	ID           string                 `json:"id"`
	Peers        []dhtRuntimeBridgeAddr `json:"peers"`
	OptionsCount int                    `json:"optionsCount"`
}

type dhtRuntimeBridgeScenario struct {
	id             string
	classification string
	rustTerminal   string
	wire           []byte
	source         netip.AddrPort
	table          dhtRuntimeBridgeTableScript
	receiveKind    string
	sendKind       string
	reportedLength int
}

type dhtRuntimeBridgeHash struct {
	id    protocol.ID
	peers []ktable.HashPeer
}

func (h dhtRuntimeBridgeHash) ID() protocol.ID { return h.id }

func (h dhtRuntimeBridgeHash) Peers() []ktable.HashPeer {
	return append([]ktable.HashPeer(nil), h.peers...)
}

func (dhtRuntimeBridgeHash) Dropped() bool { return false }

type dhtRuntimeBridgeTable struct {
	mu        sync.Mutex
	origin    protocol.ID
	script    dhtRuntimeBridgeTableScript
	calls     []dhtRuntimeBridgeTableCall
	putHashes []dhtRuntimeBridgePutHash
}

func (t *dhtRuntimeBridgeTable) Origin() protocol.ID { return t.origin }

func (t *dhtRuntimeBridgeTable) PutNode(
	protocol.ID,
	netip.AddrPort,
	...ktable.NodeOption,
) btree.PutResult {
	panic("runtime bridge responder unexpectedly called PutNode")
}

func (*dhtRuntimeBridgeTable) DropNode(protocol.ID, error) bool {
	panic("runtime bridge responder unexpectedly called DropNode")
}

func (t *dhtRuntimeBridgeTable) PutHash(
	id protocol.ID,
	peers []ktable.HashPeer,
	options ...ktable.HashOption,
) btree.PutResult {
	t.mu.Lock()
	t.calls = append(t.calls, dhtRuntimeBridgeTableCall{Method: "PutHash", ID: id.String()})
	t.mu.Unlock()
	t.recordPutHash(id, peers, len(options))
	return btree.PutAccepted
}

func (t *dhtRuntimeBridgeTable) GetClosestNodes(id protocol.ID) []ktable.Node {
	t.mu.Lock()
	t.calls = append(t.calls, dhtRuntimeBridgeTableCall{Method: "GetClosestNodes", ID: id.String()})
	t.mu.Unlock()
	return dhtRuntimeBridgeNodes(t.script.ClosestNodes)
}

func (*dhtRuntimeBridgeTable) GetOldestNodes(time.Time, int) []ktable.Node {
	panic("runtime bridge responder unexpectedly called GetOldestNodes")
}

func (*dhtRuntimeBridgeTable) GetNodesForSampleInfoHashes(int) []ktable.Node {
	panic("runtime bridge responder unexpectedly called GetNodesForSampleInfoHashes")
}

func (*dhtRuntimeBridgeTable) FilterKnownAddrs(addrs []netip.Addr) []netip.Addr {
	panic(fmt.Sprintf("runtime bridge responder unexpectedly called FilterKnownAddrs with %d addresses", len(addrs)))
}

func (t *dhtRuntimeBridgeTable) GetHashOrClosestNodes(
	id protocol.ID,
) ktable.GetHashOrClosestNodesResult {
	t.mu.Lock()
	t.calls = append(t.calls, dhtRuntimeBridgeTableCall{
		Method: "GetHashOrClosestNodes",
		ID:     id.String(),
	})
	t.mu.Unlock()
	if t.script.LookupFound {
		hashID := id
		if t.script.LookupHashID != "" {
			hashID = protocol.MustParseID(t.script.LookupHashID)
		}
		return ktable.GetHashOrClosestNodesResult{
			Found: true,
			Hash: dhtRuntimeBridgeHash{
				id:    hashID,
				peers: dhtRuntimeBridgePeers(t.script.LookupPeers),
			},
		}
	}
	return ktable.GetHashOrClosestNodesResult{
		ClosestNodes: dhtRuntimeBridgeNodes(t.script.LookupClosestNodes),
	}
}

func (t *dhtRuntimeBridgeTable) SampleHashesAndNodes() ktable.SampleHashesAndNodesResult {
	t.mu.Lock()
	t.calls = append(t.calls, dhtRuntimeBridgeTableCall{Method: "SampleHashesAndNodes"})
	t.mu.Unlock()
	hashes := make([]ktable.Hash, 0, len(t.script.SampleHashes))
	for _, id := range t.script.SampleHashes {
		hashes = append(hashes, dhtRuntimeBridgeHash{id: protocol.MustParseID(id)})
	}
	return ktable.SampleHashesAndNodesResult{
		Hashes:      hashes,
		Nodes:       dhtRuntimeBridgeNodes(t.script.SampleNodes),
		TotalHashes: int(t.script.SampleTotalHashes),
	}
}

func (t *dhtRuntimeBridgeTable) BatchCommand(commands ...ktable.Command) {
	t.mu.Lock()
	t.calls = append(t.calls, dhtRuntimeBridgeTableCall{
		Method:       "BatchCommand",
		CommandCount: len(commands),
	})
	t.mu.Unlock()
	for _, command := range commands {
		put, ok := command.(ktable.PutHash)
		if !ok {
			panic(fmt.Sprintf("runtime bridge received non-PutHash command %T", command))
		}
		t.recordPutHash(put.ID, put.Peers, len(put.Options))
	}
}

func (t *dhtRuntimeBridgeTable) recordPutHash(
	id protocol.ID,
	peers []ktable.HashPeer,
	optionsCount int,
) {
	projected := make([]dhtRuntimeBridgeAddr, 0, len(peers))
	for _, peer := range peers {
		projected = append(projected, dhtRuntimeBridgeProjectAddr(peer.Addr))
	}
	t.mu.Lock()
	t.putHashes = append(t.putHashes, dhtRuntimeBridgePutHash{
		ID: id.String(), Peers: projected, OptionsCount: optionsCount,
	})
	t.mu.Unlock()
}

func (t *dhtRuntimeBridgeTable) snapshot() []dhtRuntimeBridgePutHash {
	t.mu.Lock()
	defer t.mu.Unlock()
	return dhtRuntimeBridgeClonePutHashes(t.putHashes)
}

func (t *dhtRuntimeBridgeTable) callSnapshot() []dhtRuntimeBridgeTableCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	cloned := make([]dhtRuntimeBridgeTableCall, len(t.calls))
	copy(cloned, t.calls)
	return cloned
}

type dhtRuntimeBridgeBatchingChannel struct {
	in  chan ktable.Node
	out chan []ktable.Node
}

func newDHTRuntimeBridgeBatchingChannel() concurrency.BatchingChannel[ktable.Node] {
	return dhtRuntimeBridgeBatchingChannel{
		in:  make(chan ktable.Node, 32),
		out: make(chan []ktable.Node, 1),
	}
}

func (c dhtRuntimeBridgeBatchingChannel) In() chan<- ktable.Node { return c.in }

func (c dhtRuntimeBridgeBatchingChannel) Out() <-chan []ktable.Node { return c.out }

type dhtRuntimeBridgeResponderObservation struct {
	delegate dhtresponder.Responder
	calls    atomic.Int64
	returned atomic.Bool
	mu       sync.Mutex
	observed []dht.RecvMsg
}

func (r *dhtRuntimeBridgeResponderObservation) Respond(
	ctx context.Context,
	msg dht.RecvMsg,
) (dht.Return, error) {
	r.calls.Add(1)
	r.mu.Lock()
	r.observed = append(r.observed, msg)
	r.mu.Unlock()
	ret, err := r.delegate.Respond(ctx, msg)
	r.returned.Store(true)
	return ret, err
}

func (r *dhtRuntimeBridgeResponderObservation) observedSnapshot() []dht.RecvMsg {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]dht.RecvMsg(nil), r.observed...)
}

type dhtRuntimeBridgeSent struct {
	destination netip.AddrPort
	wire        []byte
}

type dhtRuntimeBridgeSocket struct {
	mu                  sync.Mutex
	wire                []byte
	source              netip.AddrPort
	receiveKind         string
	sendKind            string
	reportedLength      int
	receiveErr          error
	sendErr             error
	cancel              context.CancelFunc
	completed           chan struct{}
	completeOnce        sync.Once
	table               *dhtRuntimeBridgeTable
	responder           *dhtRuntimeBridgeResponderObservation
	receiveCalls        int
	sends               []dhtRuntimeBridgeSent
	atSend              []dhtRuntimeBridgePutHash
	continuationEntered atomic.Bool
	sendAfterResponder  atomic.Bool
}

func (*dhtRuntimeBridgeSocket) Open(netip.AddrPort) error { return nil }

func (*dhtRuntimeBridgeSocket) Close() error { return nil }

func (s *dhtRuntimeBridgeSocket) Receive(buffer []byte) (int, netip.AddrPort, error) {
	s.mu.Lock()
	s.receiveCalls++
	call := s.receiveCalls
	if call == 1 {
		wire := append([]byte(nil), s.wire...)
		source := s.source
		receiveKind := s.receiveKind
		reportedLength := s.reportedLength
		receiveErr := s.receiveErr
		s.mu.Unlock()
		if receiveKind == "error" {
			return 0, source, receiveErr
		}
		copy(buffer, wire)
		if receiveKind == "overreported" {
			return reportedLength, source, nil
		}
		return len(wire), source, nil
	}
	s.mu.Unlock()

	s.continuationEntered.Store(true)
	<-s.completed
	s.cancel()
	return 0, netip.AddrPort{}, context.Canceled
}

func (s *dhtRuntimeBridgeSocket) Send(destination netip.AddrPort, wire []byte) error {
	atSend := s.table.snapshot()
	s.mu.Lock()
	s.sends = append(s.sends, dhtRuntimeBridgeSent{
		destination: destination,
		wire:        append([]byte(nil), wire...),
	})
	s.atSend = atSend
	s.mu.Unlock()
	s.sendAfterResponder.Store(s.responder.returned.Load())
	s.completeOnce.Do(func() { close(s.completed) })
	if s.sendKind == "error" {
		return s.sendErr
	}
	return nil
}

type dhtRuntimeBridgeSocketSnapshot struct {
	receiveCalls int
	sends        []dhtRuntimeBridgeSent
	atSend       []dhtRuntimeBridgePutHash
}

func (s *dhtRuntimeBridgeSocket) snapshot() dhtRuntimeBridgeSocketSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	sends := make([]dhtRuntimeBridgeSent, len(s.sends))
	for index, sent := range s.sends {
		sends[index] = dhtRuntimeBridgeSent{
			destination: sent.destination,
			wire:        append([]byte(nil), sent.wire...),
		}
	}
	return dhtRuntimeBridgeSocketSnapshot{
		receiveCalls: s.receiveCalls,
		sends:        sends,
		atSend:       dhtRuntimeBridgeClonePutHashes(s.atSend),
	}
}

func TestGenerateDHTRuntimeBridgeParity(t *testing.T) {
	if strconv.IntSize != 64 {
		t.Fatalf("DHT runtime bridge requires 64-bit Go int semantics; strconv.IntSize=%d", strconv.IntSize)
	}

	secret := dhtRuntimeBridgeSecretBytes(t)
	assertDHTRuntimeBridgeActualResponderToken(t, secret)
	scenarios := dhtRuntimeBridgeScenarios(t)
	if len(scenarios) != len(dhtRuntimeBridgeFixtureIDs) {
		t.Fatalf("runtime bridge scenario count = %d, want %d", len(scenarios), len(dhtRuntimeBridgeFixtureIDs))
	}
	if len(dhtRuntimeBridgeWireGoldens) != len(dhtRuntimeBridgeFixtureIDs) {
		t.Fatalf("runtime bridge wire-golden count = %d, want %d", len(dhtRuntimeBridgeWireGoldens), len(dhtRuntimeBridgeFixtureIDs))
	}

	fixtures := make([]dhtRuntimeBridgeFixture, 0, len(scenarios))
	seen := make(map[string]struct{}, len(scenarios))
	for index, scenario := range scenarios {
		if scenario.id != dhtRuntimeBridgeFixtureIDs[index] {
			t.Fatalf("runtime bridge scenario %d ID = %q, want %q", index, scenario.id, dhtRuntimeBridgeFixtureIDs[index])
		}
		if _, exists := seen[scenario.id]; exists {
			t.Fatalf("duplicate runtime bridge scenario ID %q", scenario.id)
		}
		seen[scenario.id] = struct{}{}
		fixtures = append(fixtures, runDHTRuntimeBridgeScenario(t, scenario, secret))
	}
	reconcileDHTRuntimeBridgeFixtures(t, fixtures)
}

func dhtRuntimeBridgeScenarios(t *testing.T) []dhtRuntimeBridgeScenario {
	t.Helper()
	localID := protocol.MustParseID(dhtRuntimeBridgeNodeID)
	requester := protocol.MustParseID(dhtRuntimeBridgeRequester)
	infoHash := protocol.MustParseID(dhtRuntimeBridgeInfoHash)
	target := protocol.MustParseID(dhtRuntimeBridgeTarget)
	remote := netip.MustParseAddrPort("192.0.2.1:6881")
	mapped := netip.MustParseAddrPort("[::ffff:192.0.2.2]:6882")
	scoped := netip.MustParseAddrPort("[fe80::3%7]:6883")
	node11 := dhtRuntimeBridgeNodeValue(dhtRuntimeBridgeTarget, "192.0.2.11", 4911, 0)
	node12 := dhtRuntimeBridgeNodeValue(
		"0000000000000000000000000000000000000012",
		"198.51.100.12",
		65535,
		0,
	)
	peer21 := dhtRuntimeBridgeAddr{IP: "203.0.113.21", Port: 1, Scope: 0}
	peer22 := dhtRuntimeBridgeAddr{IP: "203.0.113.22", Port: 65535, Scope: 0}
	sample31 := "0000000000000000000000000000000000000031"
	sample32 := "0000000000000000000000000000000000000032"

	args := func() *dht.MsgArgs { return &dht.MsgArgs{ID: requester} }
	query := func(tid string, method string, arguments *dht.MsgArgs) []byte {
		return dhtRuntimeBridgeMustWire(t, dht.Msg{T: tid, Y: dht.YQuery, Q: method, A: arguments})
	}
	empty := dhtRuntimeBridgeEmptyTableScript()

	pingArgs := args()
	ping := dht.Msg{
		T: "", Y: dht.YQuery, Q: dht.QPing, A: pingArgs,
		R:        &dht.Return{ID: localID},
		E:        &dht.Error{Code: 999, Msg: "request-only"},
		ReadOnly: true,
		ClientID: string([]byte{0xff, 0}),
	}
	findArgs := args()
	findArgs.Target = target
	getArgs := args()
	getArgs.InfoHash = infoHash
	announceArgs := args()
	announceArgs.InfoHash = infoHash
	announceArgs.Token = dhtRuntimeBridgeToken
	explicitPort := 51413
	announceArgs.Port = &explicitPort
	sampleArgs := args()
	sampleArgs.Target = target

	findTable := dhtRuntimeBridgeEmptyTableScript()
	findTable.ClosestNodes = []dhtRuntimeBridgeNode{node11, node12}
	getFoundTable := dhtRuntimeBridgeEmptyTableScript()
	getFoundTable.LookupFound = true
	getFoundTable.LookupHashID = dhtRuntimeBridgeInfoHash
	getFoundTable.LookupPeers = []dhtRuntimeBridgeAddr{peer21, peer22}
	getMissTable := dhtRuntimeBridgeEmptyTableScript()
	getMissTable.LookupClosestNodes = []dhtRuntimeBridgeNode{node11, node12}
	sampleTable := dhtRuntimeBridgeEmptyTableScript()
	sampleTable.SampleHashes = []string{sample31, sample31, sample32}
	sampleTable.SampleNodes = []dhtRuntimeBridgeNode{node12, node11}
	sampleTable.SampleTotalHashes = 2

	return []dhtRuntimeBridgeScenario{
		{
			id: "ping_success_empty_tid_mixed_fields", classification: "success",
			rustTerminal: "reply_sent", wire: dhtRuntimeBridgeMustWire(t, ping), source: remote,
			table: empty, receiveKind: "datagram", sendKind: "success",
		},
		{
			id: "find_node_populated_mapped_source", classification: "success",
			rustTerminal: "reply_sent", wire: query("FN1", dht.QFindNode, findArgs), source: mapped,
			table: findTable, receiveKind: "datagram", sendKind: "success",
		},
		{
			id: "get_peers_found_values_token", classification: "success",
			rustTerminal: "reply_sent", wire: query("GP1", dht.QGetPeers, getArgs), source: remote,
			table: getFoundTable, receiveKind: "datagram", sendKind: "success",
		},
		{
			id: "get_peers_miss_nodes_token", classification: "success",
			rustTerminal: "reply_sent", wire: query("GP2", dht.QGetPeers, getArgs), source: remote,
			table: getMissTable, receiveKind: "datagram", sendKind: "success",
		},
		{
			id: "announce_peer_valid_mutates_before_send", classification: "success",
			rustTerminal: "reply_sent", wire: query("AP1", dht.QAnnouncePeer, announceArgs), source: remote,
			table: empty, receiveKind: "datagram", sendKind: "success",
		},
		{
			id: "sample_infohashes_populated_scoped_source", classification: "success",
			rustTerminal: "reply_sent", wire: query("SI1", dht.QSampleInfohashes, sampleArgs), source: scoped,
			table: sampleTable, receiveKind: "datagram", sendKind: "success",
		},
		{
			id: "unknown_method_204", classification: "protocol_204",
			rustTerminal: "reply_sent", wire: query("U1", "unknown", args()), source: remote,
			table: empty, receiveKind: "datagram", sendKind: "success",
		},
		{
			id: "missing_args_precedes_unknown_203", classification: "protocol_203",
			rustTerminal: "reply_sent", wire: query("M1", "unknown", nil), source: remote,
			table: empty, receiveKind: "datagram", sendKind: "success",
		},
		{
			id: "unsorted_duplicate_query_decode", classification: "protocol_203",
			rustTerminal: "reply_sent",
			wire:         []byte("d1:y1:q1:t2:A11:t2:A21:q4:pinge"), source: remote,
			table: empty, receiveKind: "datagram", sendKind: "success",
		},
		{
			id: "announce_send_transport_failure_mutation_survives", classification: "success",
			rustTerminal: "failed_send", wire: query("AF1", dht.QAnnouncePeer, announceArgs), source: remote,
			table: empty, receiveKind: "datagram", sendKind: "error",
		},
		{
			id: "receive_transport_error_panics", classification: "receive_transport",
			rustTerminal: "failed_receive", source: remote, table: empty,
			receiveKind: "error", sendKind: "success",
		},
		{
			id: "overreported_length_panics", classification: "overreported_length",
			rustTerminal: "failed_overreported_length", source: remote, table: empty,
			receiveKind: "overreported", sendKind: "success",
			reportedLength: 65508,
		},
	}
}

func runDHTRuntimeBridgeScenario(
	t *testing.T,
	scenario dhtRuntimeBridgeScenario,
	secret []byte,
) dhtRuntimeBridgeFixture {
	t.Helper()
	table := &dhtRuntimeBridgeTable{
		origin:    protocol.MustParseID(dhtRuntimeBridgeNodeID),
		script:    scenario.table,
		calls:     []dhtRuntimeBridgeTableCall{},
		putHashes: []dhtRuntimeBridgePutHash{},
	}
	actualResponder := dhtRuntimeBridgeNewActualResponder(t, table, secret)
	observedResponder := &dhtRuntimeBridgeResponderObservation{delegate: actualResponder}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	receiveSentinel := errors.New("runtime bridge receive sentinel")
	sendSentinel := errors.New("runtime bridge send sentinel")
	socket := &dhtRuntimeBridgeSocket{
		wire: scenario.wire, source: scenario.source,
		receiveKind: scenario.receiveKind, sendKind: scenario.sendKind,
		reportedLength: scenario.reportedLength,
		receiveErr:     receiveSentinel, sendErr: sendSentinel,
		cancel: cancel, completed: make(chan struct{}),
		table: table, responder: observedResponder,
	}
	logCore, observedLogs := observer.New(zap.DebugLevel)
	srv := &server{
		socket: socket, queries: make(map[string]pendingQuery),
		responder: observedResponder, responderTimeout: time.Minute,
		logger: zap.New(logCore).Sugar(),
	}
	panicked, recovered := dhtRuntimeBridgeInvokeRead(srv, ctx)

	if scenario.receiveKind == "datagram" {
		dhtRuntimeBridgeWait(t, func() bool {
			return socket.snapshot().receiveCalls == 2 && len(socket.snapshot().sends) == 1
		}, scenario.id+": full read/handle/send completion")
	} else if !panicked {
		t.Fatalf("%s: expected actual server.read panic", scenario.id)
	}
	if scenario.sendKind == "error" {
		dhtRuntimeBridgeWait(t, func() bool {
			return observedLogs.FilterMessage("could not send response").Len() == 1
		}, scenario.id+": send failure completion log")
	}

	socketSnapshot := socket.snapshot()
	observedMessages := observedResponder.observedSnapshot()
	var decoded dht.Msg
	decodeErr := bencode.Unmarshal(scenario.wire, &decoded)
	responderInputExact := len(observedMessages) == 1 && decodeErr == nil && reflect.DeepEqual(
		observedMessages[0],
		dht.RecvMsg{Msg: decoded, From: scenario.source},
	)
	before := []dhtRuntimeBridgePutHash{}
	after := table.snapshot()
	goTerminal := "reply_sent"
	if scenario.sendKind == "error" {
		goTerminal = "send_failure_swallowed"
	}
	expected := dhtRuntimeBridgeExpected{
		Classification: scenario.classification,
		GoTerminal:     goTerminal, RustTerminal: scenario.rustTerminal,
		ReceiveCalls:               socketSnapshot.receiveCalls,
		ResponderCalls:             int(observedResponder.calls.Load()),
		ResponderInputExact:        responderInputExact,
		SendCalls:                  len(socketSnapshot.sends),
		ContinuationReceiveEntered: socket.continuationEntered.Load(),
		SendAfterResponderReturn:   socket.sendAfterResponder.Load(),
		TableCalls:                 table.callSnapshot(),
		State: dhtRuntimeBridgeExpectedState{
			Before: before, AtSend: socketSnapshot.atSend, After: after,
		},
	}
	if panicked {
		expected.GoTerminal = "panicked"
	}
	if len(socketSnapshot.sends) == 1 {
		sent := socketSnapshot.sends[0]
		expected.DestinationPresent = true
		expected.Destination = dhtRuntimeBridgeProjectAddr(sent.destination)
		expected.WirePresent = true
		expected.WireHex = hex.EncodeToString(sent.wire)
		var roundTrip dht.Msg
		if err := bencode.Unmarshal(sent.wire, &roundTrip); err != nil {
			t.Fatalf("%s: decode actual server.send wire: %v", scenario.id, err)
		}
		canonical, err := bencode.Marshal(roundTrip)
		if err != nil || !bytes.Equal(canonical, sent.wire) {
			t.Fatalf("%s: actual server.send wire is not canonical: err=%v", scenario.id, err)
		}
		dhtRuntimeBridgeAssertReply(t, scenario, decoded, roundTrip)
	}
	if scenario.sendKind == "error" {
		entries := observedLogs.FilterMessage("could not send response").All()
		expected.SendFailureLogged = len(entries) == 1
		for _, entry := range entries {
			for _, field := range entry.Context {
				if field.Key == "retErr" && field.Interface == sendSentinel {
					expected.SendFailureIdentityExact = true
				}
			}
		}
	}
	if scenario.sendKind != "error" &&
		observedLogs.FilterMessage("could not send response").Len() != 0 {
		t.Fatalf("%s: successful socket produced a send-failure log", scenario.id)
	}
	golden, ok := dhtRuntimeBridgeWireGoldens[scenario.id]
	if !ok {
		t.Fatalf("%s: missing independent wire golden", scenario.id)
	}
	if inputHex := hex.EncodeToString(scenario.wire); inputHex != golden.inputHex {
		t.Fatalf("%s: input wire = %q, want independent golden %q", scenario.id, inputHex, golden.inputHex)
	}
	if expected.WireHex != golden.responseHex {
		t.Fatalf("%s: response wire = %q, want independent golden %q", scenario.id, expected.WireHex, golden.responseHex)
	}
	if observedLogs.FilterMessage("server error").Len() != 0 {
		t.Fatalf("%s: actual core protocol result was misclassified as a server error", scenario.id)
	}
	if panicked {
		switch scenario.receiveKind {
		case "error":
			expected.PanicClass = "receive_transport"
			if recoveredError, ok := recovered.(error); ok {
				expected.ReceivePanicRetainsTransport = errors.Is(recoveredError, receiveSentinel)
			}
		case "overreported":
			expected.PanicClass = "overreported_length"
		default:
			t.Fatalf("%s: unexpected panic from receive kind %q: %v", scenario.id, scenario.receiveKind, recovered)
		}
	}

	dhtRuntimeBridgeAssertScenario(t, scenario, expected)
	return dhtRuntimeBridgeFixture{
		ID: scenario.id, Subsystem: dhtRuntimeBridgeSubsystem,
		Runtime: dhtRuntimeBridgeRuntime{IntBits: strconv.IntSize},
		Input: dhtRuntimeBridgeInput{
			WireHex: hex.EncodeToString(scenario.wire),
			Source:  dhtRuntimeBridgeProjectAddr(scenario.source),
			Config: dhtRuntimeBridgeConfig{
				NodeID: dhtRuntimeBridgeNodeID, TokenSecretHex: dhtRuntimeBridgeSecret,
				SampleInfoHashesInterval: dhtRuntimeBridgeInterval,
			},
			Table: scenario.table,
			Socket: dhtRuntimeBridgeSocketScript{
				ReceiveKind: scenario.receiveKind, SendKind: scenario.sendKind,
				ReportedLength: scenario.reportedLength,
			},
		},
		Expected: expected,
	}
}

func dhtRuntimeBridgeAssertReply(
	t *testing.T,
	scenario dhtRuntimeBridgeScenario,
	request dht.Msg,
	reply dht.Msg,
) {
	t.Helper()
	if reply.T != request.T || reply.Y != dht.YResponse || reply.Q != "" || reply.A != nil ||
		!reflect.DeepEqual(reply.IP, dht.NodeAddr{}) || reply.ReadOnly || reply.ClientID != "" ||
		(reply.R == nil) == (reply.E == nil) {
		t.Fatalf("%s: response envelope was not clean/exclusive: request=%#v reply=%#v", scenario.id, request, reply)
	}
	if scenario.classification == "protocol_203" || scenario.classification == "protocol_204" {
		code := dht.ErrorCodeProtocolError
		message := "missing arguments"
		if scenario.classification == "protocol_204" {
			code = dht.ErrorCodeMethodUnknown
			message = "method Unknown"
		}
		if reply.R != nil || reply.E == nil || reply.E.Code != code || reply.E.Msg != message {
			t.Fatalf("%s: protocol envelope = %#v, want %d %q", scenario.id, reply, code, message)
		}
		return
	}
	if scenario.classification != "success" || reply.R == nil || reply.E != nil ||
		reply.R.ID != protocol.MustParseID(dhtRuntimeBridgeNodeID) {
		t.Fatalf("%s: success response = %#v", scenario.id, reply)
	}

	ret := reply.R
	switch scenario.id {
	case "ping_success_empty_tid_mixed_fields":
		if request.T != "" || len(ret.Nodes) != 0 || len(ret.Values) != 0 || ret.Token != nil {
			t.Fatalf("%s: unexpected ping return %#v", scenario.id, ret)
		}
	case "find_node_populated_mapped_source":
		if len(ret.Nodes) != 2 || ret.Nodes[0].ID.String() != dhtRuntimeBridgeTarget ||
			ret.Nodes[1].ID.String() != "0000000000000000000000000000000000000012" {
			t.Fatalf("%s: populated node projection %#v", scenario.id, ret.Nodes)
		}
	case "get_peers_found_values_token":
		if len(ret.Values) != 2 || len(ret.Nodes) != 0 || ret.Token == nil ||
			*ret.Token != dhtRuntimeBridgeToken {
			t.Fatalf("%s: found peer projection %#v", scenario.id, ret)
		}
	case "get_peers_miss_nodes_token":
		if len(ret.Values) != 0 || len(ret.Nodes) != 2 || ret.Token == nil ||
			*ret.Token != dhtRuntimeBridgeToken {
			t.Fatalf("%s: missed peer projection %#v", scenario.id, ret)
		}
	case "announce_peer_valid_mutates_before_send",
		"announce_send_transport_failure_mutation_survives":
		if len(ret.Nodes) != 0 || len(ret.Values) != 0 || ret.Token != nil {
			t.Fatalf("%s: announce returned unrelated fields %#v", scenario.id, ret)
		}
	case "sample_infohashes_populated_scoped_source":
		if ret.Samples == nil || len(*ret.Samples) != 3 || len(ret.Nodes) != 2 ||
			ret.Num == nil || *ret.Num != 2 || ret.Interval == nil ||
			*ret.Interval != dhtRuntimeBridgeInterval {
			t.Fatalf("%s: sample projection %#v", scenario.id, ret)
		}
	default:
		t.Fatalf("%s: success row lacks exact reply assertion", scenario.id)
	}
}

func dhtRuntimeBridgeAssertScenario(
	t *testing.T,
	scenario dhtRuntimeBridgeScenario,
	expected dhtRuntimeBridgeExpected,
) {
	t.Helper()
	if scenario.receiveKind == "datagram" {
		wantGoTerminal := "reply_sent"
		if scenario.sendKind == "error" {
			wantGoTerminal = "send_failure_swallowed"
		}
		if expected.GoTerminal != wantGoTerminal || expected.ReceiveCalls != 2 ||
			expected.ResponderCalls != 1 || !expected.ResponderInputExact ||
			expected.SendCalls != 1 || !expected.DestinationPresent || !expected.WirePresent ||
			!expected.ContinuationReceiveEntered || !expected.SendAfterResponderReturn {
			t.Fatalf("%s: incomplete production bridge evidence: %#v", scenario.id, expected)
		}
		if expected.Destination != dhtRuntimeBridgeProjectAddr(scenario.source) {
			t.Fatalf("%s: response destination changed: got %#v", scenario.id, expected.Destination)
		}
	} else if expected.GoTerminal != "panicked" || expected.ReceiveCalls != 1 ||
		expected.ResponderCalls != 0 || expected.SendCalls != 0 ||
		expected.DestinationPresent || expected.WirePresent || expected.ContinuationReceiveEntered {
		t.Fatalf("%s: incomplete receive-panic evidence: %#v", scenario.id, expected)
	}
	if scenario.receiveKind == "error" && !expected.ReceivePanicRetainsTransport {
		t.Fatalf("%s: receive panic lost exact wrapped transport", scenario.id)
	}
	if scenario.sendKind == "error" &&
		(!expected.SendFailureLogged || !expected.SendFailureIdentityExact) {
		t.Fatalf("%s: send failure log lost exact transport", scenario.id)
	}
	if scenario.sendKind != "error" &&
		(expected.SendFailureLogged || expected.SendFailureIdentityExact) {
		t.Fatalf("%s: unexpected send-failure evidence", scenario.id)
	}
	isAnnounce := scenario.id == "announce_peer_valid_mutates_before_send" ||
		scenario.id == "announce_send_transport_failure_mutation_survives"
	if isAnnounce {
		if len(expected.State.Before) != 0 || len(expected.State.AtSend) != 1 ||
			!reflect.DeepEqual(expected.State.AtSend, expected.State.After) {
			t.Fatalf("%s: announce mutation was not complete before send or survived after: %#v", scenario.id, expected.State)
		}
		put := expected.State.After[0]
		if put.ID != dhtRuntimeBridgeInfoHash || put.OptionsCount != 0 || len(put.Peers) != 1 ||
			put.Peers[0] != (dhtRuntimeBridgeAddr{IP: "192.0.2.1", Port: 51413, Scope: 0}) {
			t.Fatalf("%s: wrong exact announce mutation %#v", scenario.id, put)
		}
	} else if len(expected.State.Before) != 0 || len(expected.State.AtSend) != 0 ||
		len(expected.State.After) != 0 {
		t.Fatalf("%s: non-announce row mutated table: %#v", scenario.id, expected.State)
	}
	expectedCalls := map[string][]dhtRuntimeBridgeTableCall{
		"find_node_populated_mapped_source": {{
			Method: "GetClosestNodes", ID: dhtRuntimeBridgeTarget,
		}},
		"get_peers_found_values_token": {{
			Method: "GetHashOrClosestNodes", ID: dhtRuntimeBridgeInfoHash,
		}},
		"get_peers_miss_nodes_token": {{
			Method: "GetHashOrClosestNodes", ID: dhtRuntimeBridgeInfoHash,
		}},
		"announce_peer_valid_mutates_before_send": {{
			Method: "BatchCommand", CommandCount: 1,
		}},
		"sample_infohashes_populated_scoped_source": {{
			Method: "SampleHashesAndNodes",
		}},
		"announce_send_transport_failure_mutation_survives": {{
			Method: "BatchCommand", CommandCount: 1,
		}},
	}
	wantCalls := expectedCalls[scenario.id]
	if wantCalls == nil {
		wantCalls = []dhtRuntimeBridgeTableCall{}
	}
	if !reflect.DeepEqual(expected.TableCalls, wantCalls) {
		t.Fatalf("%s: table calls = %#v, want %#v", scenario.id, expected.TableCalls, wantCalls)
	}
}

func dhtRuntimeBridgeNewActualResponder(
	t *testing.T,
	table ktable.Table,
	secret []byte,
) dhtresponder.Responder {
	t.Helper()
	result := dhtresponder.New(dhtresponder.Params{
		KTable:          table,
		DiscoveredNodes: newDHTRuntimeBridgeBatchingChannel(),
		Logger:          zap.NewNop().Sugar(),
	})
	if !dhtRuntimeBridgeInstallPrivateSecret(reflect.ValueOf(result.Responder), secret) {
		t.Fatal("could not locate production responder tokenSecret for deterministic same-package oracle")
	}
	return result.Responder
}

// dhtRuntimeBridgeInstallPrivateSecret is an oracle-only adapter for the random
// process-lifetime secret inside the freshly constructed production responder.
// The production constructor intentionally exposes no deterministic secret
// seam. Before the responder's first use, this function follows only the
// concrete production wrapper field named `responder`, validates the exact
// private `tokenSecret []byte` field name, type, and length, and copies fixed
// bytes into that freshly allocated slice. It makes no production call or
// pointer/layout assumption, and the caller separately requires the fixed
// known-token golden before generating rows.
func dhtRuntimeBridgeInstallPrivateSecret(
	value reflect.Value,
	secret []byte,
) bool {
	if !value.IsValid() {
		return false
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return false
	}

	tokenField := value.FieldByName("tokenSecret")
	if tokenField.IsValid() {
		if tokenField.Type() != reflect.TypeOf([]byte(nil)) ||
			tokenField.Len() != len(secret) || tokenField.Len() == 0 {
			return false
		}
		copy(tokenField.Bytes(), secret)
		return true
	}

	responderField := value.FieldByName("responder")
	if !responderField.IsValid() || responderField.Kind() != reflect.Interface ||
		responderField.IsNil() {
		return false
	}
	return dhtRuntimeBridgeInstallPrivateSecret(responderField.Elem(), secret)
}

func assertDHTRuntimeBridgeActualResponderToken(t *testing.T, secret []byte) {
	t.Helper()
	table := &dhtRuntimeBridgeTable{
		origin: protocol.MustParseID(dhtRuntimeBridgeNodeID),
		script: dhtRuntimeBridgeEmptyTableScript(),
	}
	actual := dhtRuntimeBridgeNewActualResponder(t, table, secret)
	ret, err := actual.Respond(context.Background(), dht.RecvMsg{
		From: netip.MustParseAddrPort("192.0.2.1:4000"),
		Msg: dht.Msg{
			Q: dht.QGetPeers,
			A: &dht.MsgArgs{
				ID:       protocol.MustParseID(dhtRuntimeBridgeRequester),
				InfoHash: protocol.MustParseID(dhtRuntimeBridgeInfoHash),
			},
		},
	})
	if err != nil || ret.Token == nil || *ret.Token != dhtRuntimeBridgeToken {
		t.Fatalf("actual deterministic responder token = %v, %v; want %q", ret.Token, err, dhtRuntimeBridgeToken)
	}
}

func dhtRuntimeBridgeInvokeRead(srv *server, ctx context.Context) (panicked bool, recovered any) {
	defer func() {
		recovered = recover()
		panicked = recovered != nil
	}()
	srv.read(ctx)
	return false, nil
}

func dhtRuntimeBridgeWait(t *testing.T, predicate func() bool, label string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", label)
		}
		runtime.Gosched()
	}
}

func dhtRuntimeBridgeSecretBytes(t *testing.T) []byte {
	t.Helper()
	secret, err := hex.DecodeString(dhtRuntimeBridgeSecret)
	if err != nil || len(secret) != 20 {
		t.Fatalf("decode fixed runtime bridge secret: len=%d err=%v", len(secret), err)
	}
	return secret
}

func dhtRuntimeBridgeEmptyTableScript() dhtRuntimeBridgeTableScript {
	return dhtRuntimeBridgeTableScript{
		ClosestNodes:       []dhtRuntimeBridgeNode{},
		LookupFound:        false,
		LookupHashID:       "",
		LookupPeers:        []dhtRuntimeBridgeAddr{},
		LookupClosestNodes: []dhtRuntimeBridgeNode{},
		SampleHashes:       []string{},
		SampleNodes:        []dhtRuntimeBridgeNode{},
		SampleTotalHashes:  0,
	}
}

func dhtRuntimeBridgeNodes(values []dhtRuntimeBridgeNode) []ktable.Node {
	nodes := make([]ktable.Node, 0, len(values))
	for _, value := range values {
		nodes = append(nodes, ktable.NewNode(
			protocol.MustParseID(value.ID),
			dhtRuntimeBridgeAddrPort(value.Addr),
		))
	}
	return nodes
}

func dhtRuntimeBridgePeers(values []dhtRuntimeBridgeAddr) []ktable.HashPeer {
	peers := make([]ktable.HashPeer, 0, len(values))
	for _, value := range values {
		peers = append(peers, ktable.HashPeer{Addr: dhtRuntimeBridgeAddrPort(value)})
	}
	return peers
}

func dhtRuntimeBridgeAddrPort(value dhtRuntimeBridgeAddr) netip.AddrPort {
	addr := netip.MustParseAddr(value.IP)
	if value.Scope != 0 {
		addr = addr.WithZone(strconv.FormatUint(uint64(value.Scope), 10))
	}
	return netip.AddrPortFrom(addr, value.Port)
}

func dhtRuntimeBridgeProjectAddr(value netip.AddrPort) dhtRuntimeBridgeAddr {
	scope := uint32(0)
	if value.Addr().Zone() != "" {
		parsed, err := strconv.ParseUint(value.Addr().Zone(), 10, 32)
		if err != nil {
			panic(err)
		}
		scope = uint32(parsed)
	}
	return dhtRuntimeBridgeAddr{
		IP: value.Addr().WithZone("").String(), Port: value.Port(), Scope: scope,
	}
}

func dhtRuntimeBridgeNodeValue(id, ip string, port uint16, scope uint32) dhtRuntimeBridgeNode {
	return dhtRuntimeBridgeNode{ID: id, Addr: dhtRuntimeBridgeAddr{IP: ip, Port: port, Scope: scope}}
}

func dhtRuntimeBridgeMustWire(t *testing.T, message dht.Msg) []byte {
	t.Helper()
	wire, err := bencode.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func dhtRuntimeBridgeClonePutHashes(values []dhtRuntimeBridgePutHash) []dhtRuntimeBridgePutHash {
	cloned := make([]dhtRuntimeBridgePutHash, len(values))
	for index, value := range values {
		cloned[index] = dhtRuntimeBridgePutHash{
			ID: value.ID, Peers: append([]dhtRuntimeBridgeAddr(nil), value.Peers...),
			OptionsCount: value.OptionsCount,
		}
	}
	return cloned
}

func reconcileDHTRuntimeBridgeFixtures(t *testing.T, fixtures []dhtRuntimeBridgeFixture) {
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
		t.Fatal("resolve runtime bridge generator source")
	}
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source), "../../../../testdata/parity/dht/dht_runtime_bridge.jsonl",
	))
	if *updateDHTRuntimeBridgeParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read runtime bridge fixture; rerun with -update-dht-runtime-bridge-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT runtime bridge fixture is stale; rerun with -update-dht-runtime-bridge-parity")
	}
}
