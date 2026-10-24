package responder

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
	"reflect"
	"runtime"
	"strconv"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

var updateDHTResponderParity = flag.Bool(
	"update-dht-responder-parity",
	false,
	"rewrite the Rust DHT full responder parity fixture",
)

const (
	dhtResponderSubsystem  = "dht_responder"
	dhtResponderNodeID     = "00112233445566778899aabbccddeeff10203040"
	dhtResponderSecretHex  = "303132333435363738396162636465666768696a"
	dhtResponderInfoHash   = "11223344556677889900aabbccddeeff01020304"
	dhtResponderRequester  = "ffeeddccbbaa0099887766554433221100abcdef"
	dhtResponderTokenGold  = "266127f80b327ff927362ec21a79e923"
	dhtResponderMinInt64   = -1 << 63
	dhtResponderMaxInt64   = 1<<63 - 1
	dhtResponderBasePeriod = int64(10)
)

type dhtResponderFixture struct {
	ID        string               `json:"id"`
	Subsystem string               `json:"subsystem"`
	Runtime   dhtResponderRuntime  `json:"runtime"`
	Config    dhtResponderConfig   `json:"config"`
	Input     dhtResponderInput    `json:"input"`
	Expected  dhtResponderExpected `json:"expected"`
}

type dhtResponderRuntime struct {
	IntBits int `json:"intBits"`
}

type dhtResponderConfig struct {
	NodeID                   string `json:"nodeId"`
	TokenSecretHex           string `json:"tokenSecretHex"`
	SampleInfoHashesInterval int64  `json:"sampleInfoHashesInterval"`
}

type dhtResponderInput struct {
	Steps []dhtResponderStep      `json:"steps"`
	Table dhtResponderTableScript `json:"table"`
}

type dhtResponderStep struct {
	Source        dhtResponderAddr `json:"source"`
	Method        string           `json:"method"`
	ArgsPresence  string           `json:"argsPresence"`
	Args          dhtResponderArgs `json:"args"`
	TokenFromStep *int             `json:"tokenFromStep,omitempty"`
}

type dhtResponderArgs struct {
	ID           string   `json:"id"`
	InfoHash     string   `json:"infoHash"`
	Target       string   `json:"target"`
	TokenHex     string   `json:"tokenHex"`
	PortPresence string   `json:"portPresence"`
	Port         int64    `json:"port"`
	ImpliedPort  bool     `json:"impliedPort"`
	WantPresence string   `json:"wantPresence"`
	Want         []string `json:"want"`
	NoSeed       int64    `json:"noSeed"`
	Scrape       int64    `json:"scrape"`
}

type dhtResponderTableScript struct {
	ClosestNodes       []dhtResponderNode `json:"closestNodes"`
	LookupFound        bool               `json:"lookupFound"`
	LookupHashID       string             `json:"lookupHashId"`
	LookupPeers        []dhtResponderAddr `json:"lookupPeers"`
	LookupClosestNodes []dhtResponderNode `json:"lookupClosestNodes"`
	SampleHashes       []string           `json:"sampleHashes"`
	SampleNodes        []dhtResponderNode `json:"sampleNodes"`
	SampleTotalHashes  int64              `json:"sampleTotalHashes"`
}

type dhtResponderExpected struct {
	Normalization string                  `json:"normalization"`
	Outcomes      []dhtResponderOutcome   `json:"outcomes"`
	TableCalls    []dhtResponderTableCall `json:"tableCalls"`
	TableState    dhtResponderTableState  `json:"tableState"`
}

type dhtResponderOutcome struct {
	Return dhtResponderReturn `json:"return"`
	Error  *dhtResponderError `json:"error"`
}

type dhtResponderReturn struct {
	ID                   string             `json:"id"`
	NodesPresence        string             `json:"nodesPresence"`
	Nodes                []dhtResponderNode `json:"nodes"`
	Nodes6Presence       string             `json:"nodes6Presence"`
	Nodes6               []dhtResponderNode `json:"nodes6"`
	ValuesPresence       string             `json:"valuesPresence"`
	Values               []dhtResponderAddr `json:"values"`
	TokenPresence        string             `json:"tokenPresence"`
	TokenHex             string             `json:"tokenHex"`
	SamplesPresence      string             `json:"samplesPresence"`
	Samples              []string           `json:"samples"`
	NumPresence          string             `json:"numPresence"`
	Num                  int64              `json:"num"`
	IntervalPresence     string             `json:"intervalPresence"`
	Interval             int64              `json:"interval"`
	PeersBloomPresence   string             `json:"peersBloomPresence"`
	SeedersBloomPresence string             `json:"seedersBloomPresence"`
	BEP44FieldsAreZero   bool               `json:"bep44FieldsAreZero"`
}

type dhtResponderError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Text    string `json:"text"`
}

type dhtResponderTableCall struct {
	Method       string `json:"method"`
	ID           string `json:"id"`
	CommandCount int    `json:"commandCount"`
}

type dhtResponderTableState struct {
	Before dhtResponderTableSnapshot `json:"before"`
	After  dhtResponderTableSnapshot `json:"after"`
}

type dhtResponderTableSnapshot struct {
	PutHashes []dhtResponderPutHash `json:"putHashes"`
}

type dhtResponderPutHash struct {
	ID           string             `json:"id"`
	Peers        []dhtResponderAddr `json:"peers"`
	OptionsCount int                `json:"optionsCount"`
}

type dhtResponderAddr struct {
	IP   string `json:"ip"`
	Port uint16 `json:"port"`
	Zone string `json:"zone,omitempty"`
}

type dhtResponderNode struct {
	ID   string           `json:"id"`
	Addr dhtResponderAddr `json:"addr"`
}

type dhtResponderScenario struct {
	id       string
	interval int64
	steps    []dhtResponderStep
	table    dhtResponderTableScript
}

type dhtResponderScriptedHash struct {
	id    protocol.ID
	peers []ktable.HashPeer
}

func (h dhtResponderScriptedHash) ID() protocol.ID { return h.id }
func (h dhtResponderScriptedHash) Peers() []ktable.HashPeer {
	return append([]ktable.HashPeer(nil), h.peers...)
}
func (dhtResponderScriptedHash) Dropped() bool { return false }

type dhtResponderScriptedTable struct {
	ktable.Table
	script    dhtResponderTableScript
	calls     []dhtResponderTableCall
	putHashes []dhtResponderPutHash
}

func (t *dhtResponderScriptedTable) GetClosestNodes(id protocol.ID) []ktable.Node {
	t.calls = append(t.calls, dhtResponderTableCall{Method: "GetClosestNodes", ID: id.String()})
	return dhtResponderNodes(t.script.ClosestNodes)
}

func (t *dhtResponderScriptedTable) GetHashOrClosestNodes(id protocol.ID) ktable.GetHashOrClosestNodesResult {
	t.calls = append(t.calls, dhtResponderTableCall{Method: "GetHashOrClosestNodes", ID: id.String()})
	if t.script.LookupFound {
		hashID := id
		if t.script.LookupHashID != "" {
			hashID = protocol.MustParseID(t.script.LookupHashID)
		}
		return ktable.GetHashOrClosestNodesResult{
			Found: true,
			Hash: dhtResponderScriptedHash{
				id:    hashID,
				peers: dhtResponderPeers(t.script.LookupPeers),
			},
		}
	}
	return ktable.GetHashOrClosestNodesResult{
		ClosestNodes: dhtResponderNodes(t.script.LookupClosestNodes),
	}
}

func (t *dhtResponderScriptedTable) BatchCommand(commands ...ktable.Command) {
	t.calls = append(t.calls, dhtResponderTableCall{
		Method: "BatchCommand", CommandCount: len(commands),
	})
	for _, command := range commands {
		put, ok := command.(ktable.PutHash)
		if !ok {
			panic("DHT responder oracle received a non-PutHash command")
		}
		peers := make([]dhtResponderAddr, 0, len(put.Peers))
		for _, peer := range put.Peers {
			peers = append(peers, dhtResponderProjectAddr(peer.Addr))
		}
		t.putHashes = append(t.putHashes, dhtResponderPutHash{
			ID: put.ID.String(), Peers: peers, OptionsCount: len(put.Options),
		})
	}
}

func (t *dhtResponderScriptedTable) SampleHashesAndNodes() ktable.SampleHashesAndNodesResult {
	t.calls = append(t.calls, dhtResponderTableCall{Method: "SampleHashesAndNodes"})
	hashes := make([]ktable.Hash, 0, len(t.script.SampleHashes))
	for _, id := range t.script.SampleHashes {
		hashes = append(hashes, dhtResponderScriptedHash{id: protocol.MustParseID(id)})
	}
	return ktable.SampleHashesAndNodesResult{
		Hashes: hashes, Nodes: dhtResponderNodes(t.script.SampleNodes),
		TotalHashes: int(t.script.SampleTotalHashes),
	}
}

func TestGenerateDHTResponderParity(t *testing.T) {
	if strconv.IntSize != 64 {
		t.Fatalf(
			"DHT responder parity generator requires 64-bit Go int semantics for signed total and announce-port boundaries; strconv.IntSize=%d",
			strconv.IntSize,
		)
	}

	zero := protocol.ID{}.String()
	infoHash := dhtResponderInfoHash
	requester := dhtResponderRequester
	otherInfoHash := dhtResponderID(0x51)
	otherRequester := dhtResponderID(0x52)
	target := dhtResponderID(0x53)
	base := dhtResponderAddr{IP: "192.0.2.1", Port: 6881}
	baseOtherPort := dhtResponderAddr{IP: "192.0.2.1", Port: 9999}
	otherIP := dhtResponderAddr{IP: "192.0.2.2", Port: 6881}
	mapped := dhtResponderAddr{IP: "::ffff:192.0.2.1", Port: 6881}
	zone7 := dhtResponderAddr{IP: "fe80::1", Port: 6881, Zone: "7"}
	zone8 := dhtResponderAddr{IP: "fe80::1", Port: 6881, Zone: "8"}
	validTokenHex := hex.EncodeToString([]byte(dhtResponderTokenGold))

	findNodes := []dhtResponderNode{
		dhtResponderNodeValue(dhtResponderID(0x61), "192.0.2.61", 0, ""),
		dhtResponderNodeValue(dhtResponderID(0x62), "192.0.2.62", 65535, ""),
		dhtResponderNodeValue(dhtResponderID(0x61), "192.0.2.61", 0, ""),
	}
	nativeIPv6Nodes := []dhtResponderNode{
		dhtResponderNodeValue(dhtResponderID(0x75), "fe80::75", 75, "7"),
		dhtResponderNodeValue(dhtResponderID(0x76), "2001:db8::76", 65535, ""),
		dhtResponderNodeValue(dhtResponderID(0x75), "fe80::75", 75, "7"),
	}
	values := []dhtResponderAddr{
		{IP: "198.51.100.9", Port: 0},
		{IP: "2001:db8::9", Port: 65535},
		{IP: "198.51.100.9", Port: 0},
	}
	sampleHashes := []string{dhtResponderID(0x71), dhtResponderID(0x72), dhtResponderID(0x71)}
	sampleNodes := []dhtResponderNode{
		dhtResponderNodeValue(dhtResponderID(0x73), "192.0.2.73", 0, ""),
		dhtResponderNodeValue(dhtResponderID(0x74), "192.0.2.74", 65535, ""),
		dhtResponderNodeValue(dhtResponderID(0x73), "192.0.2.73", 0, ""),
	}

	scenarios := []dhtResponderScenario{
		{
			id: "global_nil_arguments_precede_unknown_method", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderNilStep(base, "not_a_method")},
		},
		{
			id: "unknown_method_with_arguments", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, "not_a_method", dhtResponderArgsValue(requester))},
		},
		{
			id: "ping_missing_arguments", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderNilStep(base, dht.QPing)},
		},
		{
			id: "ping_zero_requester_and_ignored_fields", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QPing, dhtResponderArgs{
				ID: zero, InfoHash: infoHash, Target: target,
				TokenHex: "00ff", PortPresence: "present", Port: -1, ImpliedPort: true,
				WantPresence: "present", Want: []string{"n6", "n4"}, NoSeed: -1, Scrape: 1,
			})},
		},
		{
			id: "find_node_zero_target", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QFindNode, dhtResponderArgsValue(requester))},
		},
		{
			id: "find_node_empty", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QFindNode, dhtResponderArgsWithTarget(requester, target))},
		},
		{
			id: "find_node_ordered_duplicate_nodes", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QFindNode, dhtResponderArgsWithTarget(requester, target))},
			table: dhtResponderTableScript{ClosestNodes: findNodes},
		},
		{
			id: "find_node_native_scoped_ipv6_projection", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QFindNode, dhtResponderArgsWithTarget(requester, target))},
			table: dhtResponderTableScript{ClosestNodes: nativeIPv6Nodes},
		},
		{
			id: "get_peers_zero_infohash", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QGetPeers, dhtResponderArgsValue(requester))},
		},
		{
			id: "get_peers_miss_empty", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, infoHash))},
		},
		{
			id: "get_peers_miss_ordered_duplicate_nodes", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, infoHash))},
			table: dhtResponderTableScript{LookupClosestNodes: findNodes},
		},
		{
			id: "get_peers_miss_native_scoped_ipv6_projection", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, infoHash))},
			table: dhtResponderTableScript{LookupClosestNodes: nativeIPv6Nodes},
		},
		{
			id: "get_peers_found_empty_values", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, infoHash))},
			table: dhtResponderTableScript{LookupFound: true, LookupHashID: infoHash, LookupPeers: []dhtResponderAddr{}},
		},
		{
			id: "get_peers_found_ordered_duplicate_values_ipv4_golden", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, infoHash))},
			table: dhtResponderTableScript{LookupFound: true, LookupHashID: infoHash, LookupPeers: values},
		},
		{
			id: "get_peers_ignores_scrape_want_noseed", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QGetPeers, dhtResponderArgs{
				ID: requester, InfoHash: infoHash, Target: target, TokenHex: "00ff",
				PortPresence: "present", Port: 1234, ImpliedPort: true,
				WantPresence: "present", Want: []string{"n6", "n4"}, NoSeed: dhtResponderMinInt64, Scrape: dhtResponderMaxInt64,
			})},
		},
		{
			id: "get_peers_zero_requester_token_sensitivity", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QGetPeers, dhtResponderArgsWithInfoHash(zero, infoHash))},
		},
		{
			id: "get_peers_token_port_independence", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(baseOtherPort, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, infoHash))},
		},
		{
			id: "get_peers_token_source_ip_sensitivity", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(otherIP, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, infoHash))},
		},
		{
			id: "get_peers_token_infohash_sensitivity", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, otherInfoHash))},
		},
		{
			id: "get_peers_token_requester_sensitivity", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QGetPeers, dhtResponderArgsWithInfoHash(otherRequester, infoHash))},
		},
		{
			id: "get_peers_token_mapped_ipv6_golden", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(mapped, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, infoHash))},
		},
		{
			id: "get_peers_token_native_ipv6_numeric_zone7", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(zone7, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, infoHash))},
		},
		{
			id: "get_peers_token_native_ipv6_numeric_zone8", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(zone8, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, infoHash))},
		},
		{
			id: "announce_peer_zero_infohash_no_mutation", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QAnnouncePeer, dhtResponderAnnounceArgs(requester, zero, validTokenHex, "nil", 0, false))},
		},
		{
			id: "announce_peer_bad_token_no_mutation", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QAnnouncePeer, dhtResponderAnnounceArgs(requester, infoHash, "00ff626164", "nil", 0, false))},
		},
		{
			id: "announce_peer_get_token_roundtrip_port_independent", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{
				dhtResponderStepValue(base, dht.QGetPeers, dhtResponderArgsWithInfoHash(requester, infoHash)),
				dhtResponderStepWithTokenFrom(baseOtherPort, dht.QAnnouncePeer, dhtResponderAnnounceArgs(requester, infoHash, "", "nil", 0, false), 0),
			},
		},
		dhtResponderAnnounceScenario("announce_peer_implied_port_wins", base, requester, infoHash, validTokenHex, "present", 1234, true),
		dhtResponderAnnounceScenario("announce_peer_default_source_port", base, requester, infoHash, validTokenHex, "nil", 0, false),
		dhtResponderAnnounceScenario("announce_peer_explicit_port_zero", base, requester, infoHash, validTokenHex, "present", 0, false),
		dhtResponderAnnounceScenario("announce_peer_explicit_port_65535", base, requester, infoHash, validTokenHex, "present", 65535, false),
		dhtResponderAnnounceScenario("announce_peer_explicit_port_negative_one_wraps", base, requester, infoHash, validTokenHex, "present", -1, false),
		dhtResponderAnnounceScenario("announce_peer_explicit_port_65536_wraps", base, requester, infoHash, validTokenHex, "present", 65536, false),
		dhtResponderAnnounceScenario("announce_peer_explicit_port_i64_min_wraps", base, requester, infoHash, validTokenHex, "present", dhtResponderMinInt64, false),
		dhtResponderAnnounceScenario("announce_peer_explicit_port_i64_max_wraps", base, requester, infoHash, validTokenHex, "present", dhtResponderMaxInt64, false),
		{
			id: "sample_infohashes_nil_arguments", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderNilStep(base, dht.QSampleInfohashes)},
		},
		{
			id: "sample_infohashes_zero_target_empty_present_fields", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QSampleInfohashes, dhtResponderArgsValue(requester))},
			table: dhtResponderTableScript{SampleHashes: []string{}, SampleNodes: []dhtResponderNode{}},
		},
		{
			id: "sample_infohashes_ordered_duplicate_hashes_and_nodes", interval: 20,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QSampleInfohashes, dhtResponderArgs{
				ID: requester, InfoHash: infoHash, Target: target, TokenHex: "00ff",
				PortPresence: "present", Port: -1, ImpliedPort: true,
				WantPresence: "present", Want: []string{"n4", "n6"}, NoSeed: -1, Scrape: 1,
			})},
			table: dhtResponderTableScript{SampleHashes: sampleHashes, SampleNodes: sampleNodes, SampleTotalHashes: 123},
		},
		{
			id: "sample_infohashes_native_scoped_ipv6_projection", interval: dhtResponderBasePeriod,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QSampleInfohashes, dhtResponderArgsWithTarget(requester, target))},
			table: dhtResponderTableScript{SampleHashes: []string{}, SampleNodes: nativeIPv6Nodes},
		},
		{
			id: "sample_infohashes_signed_i64_min_total_and_interval", interval: dhtResponderMinInt64,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QSampleInfohashes, dhtResponderArgsValue(requester))},
			table: dhtResponderTableScript{SampleTotalHashes: dhtResponderMinInt64},
		},
		{
			id: "sample_infohashes_signed_i64_max_total_and_interval", interval: dhtResponderMaxInt64,
			steps: []dhtResponderStep{dhtResponderStepValue(base, dht.QSampleInfohashes, dhtResponderArgsValue(requester))},
			table: dhtResponderTableScript{SampleTotalHashes: dhtResponderMaxInt64},
		},
	}

	fixtures := make([]dhtResponderFixture, 0, len(scenarios))
	seen := make(map[string]struct{}, len(scenarios))
	for _, scenario := range scenarios {
		if _, exists := seen[scenario.id]; exists {
			t.Fatalf("duplicate DHT responder fixture ID %q", scenario.id)
		}
		seen[scenario.id] = struct{}{}
		fixtures = append(fixtures, runDHTResponderScenario(t, scenario))
	}
	dhtResponderAssertCoverage(t, fixtures)
	reconcileDHTResponderFixtures(t, fixtures)
}

func runDHTResponderScenario(t *testing.T, scenario dhtResponderScenario) dhtResponderFixture {
	t.Helper()
	secret, err := hex.DecodeString(dhtResponderSecretHex)
	if err != nil || len(secret) != 20 {
		t.Fatalf("fixed token secret must be exactly 20 bytes: len=%d err=%v", len(secret), err)
	}
	tokenSecret := make([]byte, len(secret))
	copy(tokenSecret, secret)
	tokenSecret = tokenSecret[:len(tokenSecret):len(tokenSecret)]
	table := &dhtResponderScriptedTable{
		script:    scenario.table,
		calls:     []dhtResponderTableCall{},
		putHashes: []dhtResponderPutHash{},
	}
	actual := responder{
		nodeID: protocol.MustParseID(dhtResponderNodeID), kTable: table,
		tokenSecret: tokenSecret, sampleInfoHashesInterval: scenario.interval,
	}
	outcomes := make([]dhtResponderOutcome, 0, len(scenario.steps))
	for index, step := range scenario.steps {
		args := dhtResponderMessageArgs(t, step)
		if step.TokenFromStep != nil {
			from := *step.TokenFromStep
			if from < 0 || from >= len(outcomes) {
				t.Fatalf("%s step %d has invalid tokenFromStep %d", scenario.id, index, from)
			}
			token, decodeErr := hex.DecodeString(outcomes[from].Return.TokenHex)
			if decodeErr != nil || outcomes[from].Return.TokenPresence != "present" {
				t.Fatalf("%s step %d cannot reuse absent/invalid token from step %d", scenario.id, index, from)
			}
			args.Token = string(token)
		}
		var messageArgs *dht.MsgArgs
		if step.ArgsPresence == "present" {
			messageArgs = &args
		} else if step.ArgsPresence != "nil" {
			t.Fatalf("%s step %d invalid args presence %q", scenario.id, index, step.ArgsPresence)
		}
		ret, respondErr := actual.Respond(context.Background(), dht.RecvMsg{
			From: dhtResponderAddrPort(step.Source),
			Msg:  dht.Msg{Q: step.Method, A: messageArgs},
		})
		outcomes = append(outcomes, dhtResponderProjectOutcome(t, ret, respondErr))
	}
	return dhtResponderFixture{
		ID: scenario.id, Subsystem: dhtResponderSubsystem,
		Runtime: dhtResponderRuntime{IntBits: strconv.IntSize},
		Config: dhtResponderConfig{
			NodeID: dhtResponderNodeID, TokenSecretHex: dhtResponderSecretHex,
			SampleInfoHashesInterval: scenario.interval,
		},
		Input: dhtResponderInput{Steps: scenario.steps, Table: scenario.table},
		Expected: dhtResponderExpected{
			Normalization: "none",
			Outcomes:      outcomes,
			TableCalls:    table.calls,
			TableState: dhtResponderTableState{
				Before: dhtResponderTableSnapshot{PutHashes: []dhtResponderPutHash{}},
				After:  dhtResponderTableSnapshot{PutHashes: table.putHashes},
			},
		},
	}
}

func dhtResponderMessageArgs(t *testing.T, step dhtResponderStep) dht.MsgArgs {
	t.Helper()
	token, err := hex.DecodeString(step.Args.TokenHex)
	if err != nil {
		t.Fatalf("decode token hex: %v", err)
	}
	var port *int
	if step.Args.PortPresence == "present" {
		value := int(step.Args.Port)
		port = &value
	} else if step.Args.PortPresence != "nil" {
		t.Fatalf("invalid port presence %q", step.Args.PortPresence)
	}
	var want []dht.Want
	if step.Args.WantPresence != "nil" {
		want = make([]dht.Want, 0, len(step.Args.Want))
		for _, value := range step.Args.Want {
			want = append(want, dht.Want(value))
		}
		if step.Args.WantPresence != "empty" && step.Args.WantPresence != "present" {
			t.Fatalf("invalid want presence %q", step.Args.WantPresence)
		}
	}
	return dht.MsgArgs{
		ID: protocol.MustParseID(step.Args.ID), InfoHash: protocol.MustParseID(step.Args.InfoHash),
		Target: protocol.MustParseID(step.Args.Target), Token: string(token), Port: port,
		ImpliedPort: step.Args.ImpliedPort, Want: want,
		NoSeed: int(step.Args.NoSeed), Scrape: int(step.Args.Scrape),
	}
}

func dhtResponderProjectOutcome(t *testing.T, ret dht.Return, err error) dhtResponderOutcome {
	t.Helper()
	outcome := dhtResponderOutcome{Return: dhtResponderProjectReturn(ret)}
	if err == nil {
		return outcome
	}
	var protocolError dht.Error
	if !errors.As(err, &protocolError) {
		t.Fatalf("actual responder returned non-protocol error %T: %v", err, err)
	}
	outcome.Error = &dhtResponderError{
		Code: protocolError.Code, Message: protocolError.Msg, Text: err.Error(),
	}
	return outcome
}

func dhtResponderProjectReturn(ret dht.Return) dhtResponderReturn {
	projected := dhtResponderReturn{
		ID:                   ret.ID.String(),
		NodesPresence:        dhtResponderSlicePresence(ret.Nodes),
		Nodes:                dhtResponderProjectNodeInfos(ret.Nodes),
		Nodes6Presence:       dhtResponderSlicePresence(ret.Nodes6),
		Nodes6:               dhtResponderProjectNodeInfos(ret.Nodes6),
		ValuesPresence:       dhtResponderSlicePresence(ret.Values),
		Values:               dhtResponderProjectNodeAddrs(ret.Values),
		TokenPresence:        dhtResponderPointerPresence(ret.Token),
		SamplesPresence:      dhtResponderSamplesPresence(ret.Samples),
		NumPresence:          dhtResponderPointerPresence(ret.Num),
		IntervalPresence:     dhtResponderPointerPresence(ret.Interval),
		PeersBloomPresence:   dhtResponderPointerPresence(ret.BFpe),
		SeedersBloomPresence: dhtResponderPointerPresence(ret.BFsd),
		BEP44FieldsAreZero:   reflect.DeepEqual(ret.Bep44Return, dht.Bep44Return{}),
	}
	if ret.Token != nil {
		projected.TokenHex = hex.EncodeToString([]byte(*ret.Token))
	}
	if ret.Samples != nil {
		projected.Samples = make([]string, 0, len(*ret.Samples))
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

func dhtResponderAssertCoverage(t *testing.T, fixtures []dhtResponderFixture) {
	t.Helper()
	if len(fixtures) != 40 {
		t.Fatalf("DHT responder oracle case count changed: got %d want 40", len(fixtures))
	}
	byID := make(map[string]dhtResponderFixture, len(fixtures))
	for _, fixture := range fixtures {
		byID[fixture.ID] = fixture
		if fixture.Runtime.IntBits != 64 {
			t.Fatalf("%s: runtime int width is not fixed to 64", fixture.ID)
		}
	}
	token := func(id string) string {
		fixture, ok := byID[id]
		if !ok || len(fixture.Expected.Outcomes) == 0 {
			t.Fatalf("missing token fixture %q", id)
		}
		bytes, err := hex.DecodeString(fixture.Expected.Outcomes[0].Return.TokenHex)
		if err != nil {
			t.Fatalf("%s: invalid token hex: %v", id, err)
		}
		return string(bytes)
	}
	base := token("get_peers_found_ordered_duplicate_values_ipv4_golden")
	if base != dhtResponderTokenGold {
		t.Fatalf("IPv4 token golden changed: got %q want %q", base, dhtResponderTokenGold)
	}
	if token("get_peers_token_port_independence") != base {
		t.Fatal("announce token unexpectedly depends on UDP source port")
	}
	for _, id := range []string{
		"get_peers_zero_requester_token_sensitivity",
		"get_peers_token_source_ip_sensitivity",
		"get_peers_token_infohash_sensitivity",
		"get_peers_token_requester_sensitivity",
		"get_peers_token_mapped_ipv6_golden",
		"get_peers_token_native_ipv6_numeric_zone7",
		"get_peers_token_native_ipv6_numeric_zone8",
	} {
		if token(id) == base {
			t.Fatalf("%s: expected token sensitivity", id)
		}
	}
	if token("get_peers_token_native_ipv6_numeric_zone7") == token("get_peers_token_native_ipv6_numeric_zone8") {
		t.Fatal("native IPv6 token unexpectedly ignores numeric zone")
	}
	for _, fixture := range fixtures {
		if fixture.Expected.Normalization != "none" {
			t.Fatalf("%s: scripted responder order must not be normalized", fixture.ID)
		}
		if len(fixture.Expected.TableState.Before.PutHashes) != 0 {
			t.Fatalf("%s: scripted table must start empty", fixture.ID)
		}
		if len(fixture.Expected.TableState.After.PutHashes) > 1 {
			t.Fatalf("%s: responder issued more than one PutHash", fixture.ID)
		}
		successfulAnnounces := 0
		for index, outcome := range fixture.Expected.Outcomes {
			if fixture.Input.Steps[index].Method == dht.QAnnouncePeer && outcome.Error == nil {
				successfulAnnounces++
			}
		}
		batchCalls := 0
		for _, call := range fixture.Expected.TableCalls {
			if call.Method == "BatchCommand" {
				batchCalls++
				if call.CommandCount != 1 {
					t.Fatalf("%s: announce BatchCommand contains %d commands, want 1", fixture.ID, call.CommandCount)
				}
			}
		}
		if successfulAnnounces != len(fixture.Expected.TableState.After.PutHashes) ||
			successfulAnnounces != batchCalls {
			t.Fatalf(
				"%s: successful announces=%d PutHashes=%d BatchCommands=%d",
				fixture.ID,
				successfulAnnounces,
				len(fixture.Expected.TableState.After.PutHashes),
				batchCalls,
			)
		}
	}
}

func dhtResponderAnnounceScenario(
	id string,
	source dhtResponderAddr,
	requester string,
	infoHash string,
	tokenHex string,
	portPresence string,
	port int64,
	implied bool,
) dhtResponderScenario {
	return dhtResponderScenario{
		id: id, interval: dhtResponderBasePeriod,
		steps: []dhtResponderStep{dhtResponderStepValue(
			source, dht.QAnnouncePeer,
			dhtResponderAnnounceArgs(requester, infoHash, tokenHex, portPresence, port, implied),
		)},
	}
}

func dhtResponderArgsValue(id string) dhtResponderArgs {
	zero := protocol.ID{}.String()
	return dhtResponderArgs{
		ID: id, InfoHash: zero, Target: zero,
		PortPresence: "nil", WantPresence: "nil",
	}
}

func dhtResponderArgsWithInfoHash(id string, infoHash string) dhtResponderArgs {
	args := dhtResponderArgsValue(id)
	args.InfoHash = infoHash
	return args
}

func dhtResponderArgsWithTarget(id string, target string) dhtResponderArgs {
	args := dhtResponderArgsValue(id)
	args.Target = target
	return args
}

func dhtResponderAnnounceArgs(
	id string,
	infoHash string,
	tokenHex string,
	portPresence string,
	port int64,
	implied bool,
) dhtResponderArgs {
	args := dhtResponderArgsWithInfoHash(id, infoHash)
	args.TokenHex = tokenHex
	args.PortPresence = portPresence
	args.Port = port
	args.ImpliedPort = implied
	return args
}

func dhtResponderNilStep(source dhtResponderAddr, method string) dhtResponderStep {
	return dhtResponderStep{
		Source: source, Method: method, ArgsPresence: "nil",
		Args: dhtResponderArgsValue(protocol.ID{}.String()),
	}
}

func dhtResponderStepValue(source dhtResponderAddr, method string, args dhtResponderArgs) dhtResponderStep {
	return dhtResponderStep{Source: source, Method: method, ArgsPresence: "present", Args: args}
}

func dhtResponderStepWithTokenFrom(
	source dhtResponderAddr,
	method string,
	args dhtResponderArgs,
	from int,
) dhtResponderStep {
	step := dhtResponderStepValue(source, method, args)
	step.TokenFromStep = &from
	return step
}

func dhtResponderID(last byte) string {
	id := protocol.ID{}
	id[19] = last
	return id.String()
}

func dhtResponderNodeValue(id string, ip string, port uint16, zone string) dhtResponderNode {
	return dhtResponderNode{ID: id, Addr: dhtResponderAddr{IP: ip, Port: port, Zone: zone}}
}

func dhtResponderAddrPort(value dhtResponderAddr) netip.AddrPort {
	addr := netip.MustParseAddr(value.IP)
	if value.Zone != "" {
		addr = addr.WithZone(value.Zone)
	}
	return netip.AddrPortFrom(addr, value.Port)
}

func dhtResponderProjectAddr(value netip.AddrPort) dhtResponderAddr {
	addr := value.Addr()
	zone := addr.Zone()
	if zone != "" {
		addr = addr.WithZone("")
	}
	return dhtResponderAddr{
		IP: addr.String(), Port: value.Port(), Zone: zone,
	}
}

func dhtResponderNodes(values []dhtResponderNode) []ktable.Node {
	if values == nil {
		return nil
	}
	nodes := make([]ktable.Node, 0, len(values))
	for _, value := range values {
		nodes = append(nodes, ktable.NewNode(protocol.MustParseID(value.ID), dhtResponderAddrPort(value.Addr)))
	}
	return nodes
}

func dhtResponderPeers(values []dhtResponderAddr) []ktable.HashPeer {
	if values == nil {
		return nil
	}
	peers := make([]ktable.HashPeer, 0, len(values))
	for _, value := range values {
		peers = append(peers, ktable.HashPeer{Addr: dhtResponderAddrPort(value)})
	}
	return peers
}

func dhtResponderProjectNodeInfos(values []dht.NodeInfo) []dhtResponderNode {
	if values == nil {
		return nil
	}
	nodes := make([]dhtResponderNode, 0, len(values))
	for _, value := range values {
		nodes = append(nodes, dhtResponderNode{
			ID: value.ID.String(), Addr: dhtResponderProjectAddr(value.Addr.ToAddrPort()),
		})
	}
	return nodes
}

func dhtResponderProjectNodeAddrs(values []dht.NodeAddr) []dhtResponderAddr {
	if values == nil {
		return nil
	}
	addrs := make([]dhtResponderAddr, 0, len(values))
	for _, value := range values {
		addrs = append(addrs, dhtResponderProjectAddr(value.ToAddrPort()))
	}
	return addrs
}

func dhtResponderSlicePresence[T any](value []T) string {
	if value == nil {
		return "nil"
	}
	if len(value) == 0 {
		return "empty"
	}
	return "present"
}

func dhtResponderSamplesPresence(value *dht.CompactInfohashes) string {
	if value == nil {
		return "nil"
	}
	if len(*value) == 0 {
		return "empty"
	}
	return "present"
}

func dhtResponderPointerPresence[T any](value *T) string {
	if value == nil {
		return "nil"
	}
	return "present"
}

func reconcileDHTResponderFixtures(t *testing.T, fixtures []dhtResponderFixture) {
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
		filepath.Dir(source), "../../../../testdata/parity/dht/dht_responder.jsonl",
	))
	if *updateDHTResponderParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-responder-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT responder fixture is stale; rerun with -update-dht-responder-parity")
	}
}
