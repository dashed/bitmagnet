package client

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"github.com/bits-and-blooms/bloom/v3"
)

var updateDHTPeerSampleClientParity = flag.Bool(
	"update-dht-peer-sample-client-parity",
	false,
	"rewrite the Rust DHT get-peers/scrape/sample-infohashes client fixture",
)

const peerSampleClientSubsystem = "dht_peer_sample_client"

type peerSampleClientFixture struct {
	ID        string                   `json:"id"`
	Subsystem string                   `json:"subsystem"`
	Runtime   peerSampleClientRuntime  `json:"runtime"`
	Input     peerSampleClientInput    `json:"input"`
	Expected  peerSampleClientExpected `json:"expected"`
}

type peerSampleClientRuntime struct {
	IntBits int `json:"intBits"`
}

type peerSampleClientInput struct {
	Operation        string                        `json:"operation"`
	TransactionIDHex string                        `json:"transactionIdHex"`
	LocalID          string                        `json:"localId"`
	Remote           peerSampleClientAddr          `json:"remote"`
	InfoHash         *string                       `json:"infoHash,omitempty"`
	Target           *string                       `json:"target,omitempty"`
	Response         peerSampleClientResponseInput `json:"response"`
	Failure          string                        `json:"failure,omitempty"`
}

type peerSampleClientResponseInput struct {
	ID                   string                 `json:"id"`
	NodesPresence        string                 `json:"nodesPresence"`
	Nodes                []peerSampleClientNode `json:"nodes"`
	Nodes6Presence       string                 `json:"nodes6Presence"`
	Nodes6               []peerSampleClientNode `json:"nodes6"`
	ValuesPresence       string                 `json:"valuesPresence"`
	Values               []peerSampleClientAddr `json:"values"`
	TokenPresence        string                 `json:"tokenPresence"`
	TokenHex             string                 `json:"tokenHex"`
	SamplesPresence      string                 `json:"samplesPresence"`
	Samples              []string               `json:"samples"`
	NumPresence          string                 `json:"numPresence"`
	Num                  int64                  `json:"num"`
	IntervalPresence     string                 `json:"intervalPresence"`
	Interval             int64                  `json:"interval"`
	PeersBloomPresence   string                 `json:"peersBloomPresence"`
	PeersBloomHex        string                 `json:"peersBloomHex"`
	SeedersBloomPresence string                 `json:"seedersBloomPresence"`
	SeedersBloomHex      string                 `json:"seedersBloomHex"`
}

type peerSampleClientExpected struct {
	QueryCalls             int                       `json:"queryCalls"`
	QueryMethod            string                    `json:"queryMethod"`
	QueryRemote            peerSampleClientAddr      `json:"queryRemote"`
	QueryArgs              peerSampleClientQueryArgs `json:"queryArgs"`
	QueryWireHex           string                    `json:"queryWireHex"`
	Outcome                string                    `json:"outcome"`
	ErrorText              string                    `json:"errorText"`
	ErrorIdentityPreserved bool                      `json:"errorIdentityPreserved"`
	ErrorIsTypedNil        bool                      `json:"errorIsTypedNil"`
	ResultWasZero          bool                      `json:"resultWasZero"`
	Result                 peerSampleClientResult    `json:"result"`
}

type peerSampleClientQueryArgs struct {
	ID                 string   `json:"id"`
	InfoHash           string   `json:"infoHash"`
	Target             string   `json:"target"`
	TokenHex           string   `json:"tokenHex"`
	PortPresence       string   `json:"portPresence"`
	ImpliedPort        bool     `json:"impliedPort"`
	WantPresence       string   `json:"wantPresence"`
	Want               []string `json:"want"`
	NoSeed             int      `json:"noSeed"`
	Scrape             int      `json:"scrape"`
	BEP44FieldsAreZero bool     `json:"bep44FieldsAreZero"`
}

type peerSampleClientResult struct {
	ID              string                       `json:"id"`
	NodesPresence   string                       `json:"nodesPresence"`
	Nodes           []peerSampleClientNode       `json:"nodes"`
	ValuesPresence  string                       `json:"valuesPresence"`
	Values          []peerSampleClientAddr       `json:"values"`
	SamplesPresence string                       `json:"samplesPresence"`
	Samples         []string                     `json:"samples"`
	Num             int                          `json:"num"`
	Interval        int                          `json:"interval"`
	PeersBloom      *peerSampleClientBloomResult `json:"peersBloom,omitempty"`
	SeedersBloom    *peerSampleClientBloomResult `json:"seedersBloom,omitempty"`
}

type peerSampleClientBloomResult struct {
	BloomHex         string `json:"bloomHex"`
	Capacity         uint   `json:"capacity"`
	Hashes           uint   `json:"hashes"`
	ApproximatedSize uint32 `json:"approximatedSize"`
}

type peerSampleClientAddr struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope,omitempty"`
}

type peerSampleClientNode struct {
	ID   string               `json:"id"`
	Addr peerSampleClientAddr `json:"addr"`
}

type peerSampleClientScenario struct {
	id            string
	operation     string
	transactionID string
	localID       protocol.ID
	remote        netip.AddrPort
	infoHash      protocol.ID
	target        protocol.ID
	response      dht.Return
	failure       string
}

type peerSampleClientCapturedQuery struct {
	remote netip.AddrPort
	method string
	args   dht.MsgArgs
	wire   []byte
}

// Embedding the sealed production interface promotes its private lifecycle
// methods while Query is scripted. This invokes the actual unexported
// serverAdapter without opening a socket or changing production source.
type peerSampleClientScriptedServer struct {
	server.Server
	transactionID string
	response      dht.RecvMsg
	queryErr      error
	queries       []peerSampleClientCapturedQuery
}

func (s *peerSampleClientScriptedServer) Query(
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
	s.queries = append(s.queries, peerSampleClientCapturedQuery{
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

func TestGenerateDHTPeerSampleClientParity(t *testing.T) {
	if strconv.IntSize != 64 {
		t.Fatalf(
			"peer/sample client parity generator requires 64-bit Go int semantics for signed min/max cases; strconv.IntSize=%d",
			strconv.IntSize,
		)
	}

	localID := peerSampleClientID(0x11)
	infoHash := peerSampleClientID(0x22)
	target := peerSampleClientID(0x23)
	responseID := peerSampleClientID(0x33)
	nodeA := peerSampleClientNodeInfo(0x41, "192.0.2.40", 0)
	nodeB := peerSampleClientNodeInfo(0x42, "192.0.2.40", 65535)
	nodeV6 := peerSampleClientNodeInfo(0x43, "2001:db8::43", 6881)
	valueA := peerSampleClientNodeAddr("198.51.100.9", 0)
	valueB := peerSampleClientNodeAddr("2001:db8::9", 65535)
	patternPeers := peerSampleClientPatternBloom(37, 11)
	patternSeeders := peerSampleClientPatternBloom(73, 19)
	zeroBloom := new(dht.ScrapeBloomFilter)
	token := "ignored\x00token"
	ignoredInterval := int64(17)
	ignoredNum := int64(19)
	ignoredSamples := dht.CompactInfohashes{peerSampleClientID(0x55)}

	scenarios := []peerSampleClientScenario{
		{
			id: "get_peers_ordered_duplicate_nodes_and_values", operation: "get_peers",
			transactionID: "G1", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.1:6881"), infoHash: infoHash,
			response: dht.Return{
				ID:     responseID,
				Nodes:  dht.CompactIPv4NodeInfo{nodeA, nodeB, nodeA},
				Values: []dht.NodeAddr{valueA, valueB, valueA},
			},
		},
		{
			id: "get_peers_zero_infohash_and_zero_response_id", operation: "get_peers",
			transactionID: "G2", localID: localID,
			remote:   netip.MustParseAddrPort("192.0.2.2:0"),
			response: dht.Return{},
		},
		{
			id: "get_peers_nil_collections", operation: "get_peers",
			transactionID: "G3", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.3:6883"), infoHash: infoHash,
			response: dht.Return{ID: responseID},
		},
		{
			id: "get_peers_present_empty_collections", operation: "get_peers",
			transactionID: "G4", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.4:6884"), infoHash: infoHash,
			response: dht.Return{
				ID: responseID, Nodes: dht.CompactIPv4NodeInfo{}, Values: []dht.NodeAddr{},
			},
		},
		{
			id: "get_peers_ignores_nodes6_token_and_extensions", operation: "get_peers",
			transactionID: "G5", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.5:6885"), infoHash: infoHash,
			response: dht.Return{
				ID: responseID, Nodes6: dht.CompactIPv6NodeInfo{nodeV6}, Token: &token,
				BFpe: patternPeers, BFsd: patternSeeders,
				Bep51Return: dht.Bep51Return{
					Interval: &ignoredInterval, Num: &ignoredNum, Samples: &ignoredSamples,
				},
			},
		},
		{
			id: "get_peers_query_error_identity", operation: "get_peers",
			transactionID: "G6", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.6:6886"), infoHash: infoHash,
			response: dht.Return{ID: responseID, Values: []dht.NodeAddr{valueA}}, failure: "query_error",
		},
		{
			id: "get_peers_pre_cancelled_context_still_queries", operation: "get_peers",
			transactionID: "G7", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.7:6887"), infoHash: infoHash,
			response: dht.Return{ID: responseID}, failure: "pre_cancelled",
		},
		{
			id: "get_peers_typed_nil_query_error", operation: "get_peers",
			transactionID: "G8", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.8:6888"), infoHash: infoHash,
			response: dht.Return{ID: responseID}, failure: "typed_nil_error",
		},
		{
			id: "scrape_patterned_filters_preserve_direction", operation: "get_peers_scrape",
			transactionID: "B1", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.11:6891"), infoHash: infoHash,
			response: dht.Return{
				ID: responseID, Nodes: dht.CompactIPv4NodeInfo{nodeA}, Values: []dht.NodeAddr{valueA},
				Nodes6: dht.CompactIPv6NodeInfo{nodeV6}, Token: &token,
				BFpe: patternPeers, BFsd: patternSeeders,
				Bep51Return: dht.Bep51Return{Interval: &ignoredInterval, Samples: &ignoredSamples},
			},
		},
		{
			id: "scrape_all_zero_filters_are_present", operation: "get_peers_scrape",
			transactionID: "B2", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.12:6892"), infoHash: infoHash,
			response: dht.Return{ID: responseID, BFpe: zeroBloom, BFsd: zeroBloom},
		},
		{
			id: "scrape_missing_peers_filter", operation: "get_peers_scrape",
			transactionID: "B3", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.13:6893"), infoHash: infoHash,
			response: dht.Return{ID: responseID, BFsd: patternSeeders},
		},
		{
			id: "scrape_missing_seeders_filter", operation: "get_peers_scrape",
			transactionID: "B4", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.14:6894"), infoHash: infoHash,
			response: dht.Return{ID: responseID, BFpe: patternPeers},
		},
		{
			id: "scrape_missing_both_filters", operation: "get_peers_scrape",
			transactionID: "B5", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.15:6895"), infoHash: infoHash,
			response: dht.Return{ID: responseID},
		},
		{
			id: "scrape_query_error_precedes_missing_filters", operation: "get_peers_scrape",
			transactionID: "B6", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.16:6896"), infoHash: infoHash,
			response: dht.Return{ID: responseID}, failure: "query_error",
		},
		{
			id: "sample_ordered_duplicate_samples_and_nodes", operation: "sample_infohashes",
			transactionID: "S1", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.21:6901"), target: target,
			response: peerSampleClientSampleReturn(
				responseID,
				dht.CompactInfohashes{peerSampleClientID(0x61), peerSampleClientID(0x62), peerSampleClientID(0x61)},
				[]dht.NodeInfo{nodeA, nodeB, nodeA},
				peerSampleClientInt64(3), peerSampleClientInt64(300),
			),
		},
		{
			id: "sample_zero_target_and_zero_response_id", operation: "sample_infohashes",
			transactionID: "S2", localID: localID,
			remote:   netip.MustParseAddrPort("192.0.2.22:6902"),
			response: dht.Return{},
		},
		{
			id: "sample_nil_samples_and_absent_counts_default", operation: "sample_infohashes",
			transactionID: "S3", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.23:6903"), target: target,
			response: dht.Return{ID: responseID},
		},
		{
			id: "sample_present_empty_samples", operation: "sample_infohashes",
			transactionID: "S4", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.24:6904"), target: target,
			response: peerSampleClientSampleReturn(
				responseID, dht.CompactInfohashes{}, nil, nil, nil,
			),
		},
		{
			id: "sample_num_present_interval_absent", operation: "sample_infohashes",
			transactionID: "S5", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.25:6905"), target: target,
			response: peerSampleClientSampleReturn(responseID, nil, nil, peerSampleClientInt64(7), nil),
		},
		{
			id: "sample_interval_present_num_absent", operation: "sample_infohashes",
			transactionID: "S6", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.26:6906"), target: target,
			response: peerSampleClientSampleReturn(responseID, nil, nil, nil, peerSampleClientInt64(9)),
		},
		{
			id: "sample_negative_num_and_interval", operation: "sample_infohashes",
			transactionID: "S7", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.27:6907"), target: target,
			response: peerSampleClientSampleReturn(
				responseID, dht.CompactInfohashes{peerSampleClientID(0x63)}, nil,
				peerSampleClientInt64(-5), peerSampleClientInt64(-7),
			),
		},
		{
			id: "sample_i64_max_counts", operation: "sample_infohashes",
			transactionID: "S8", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.28:6908"), target: target,
			response: peerSampleClientSampleReturn(
				responseID, nil, nil, peerSampleClientInt64(int64(^uint64(0)>>1)),
				peerSampleClientInt64(int64(^uint64(0)>>1)),
			),
		},
		{
			id: "sample_i64_min_counts", operation: "sample_infohashes",
			transactionID: "S9", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.29:6909"), target: target,
			response: peerSampleClientSampleReturn(
				responseID, nil, nil, peerSampleClientInt64(-int64(^uint64(0)>>1)-1),
				peerSampleClientInt64(-int64(^uint64(0)>>1)-1),
			),
		},
		{
			id: "sample_inconsistent_num_and_sample_count", operation: "sample_infohashes",
			transactionID: "SA", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.30:6910"), target: target,
			response: peerSampleClientSampleReturn(
				responseID,
				dht.CompactInfohashes{peerSampleClientID(0x64), peerSampleClientID(0x65), peerSampleClientID(0x66)},
				nil, peerSampleClientInt64(1), peerSampleClientInt64(0),
			),
		},
		{
			id: "sample_ignores_values_nodes6_token_and_blooms", operation: "sample_infohashes",
			transactionID: "SB", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.31:6911"), target: target,
			response: dht.Return{
				ID: responseID, Nodes6: dht.CompactIPv6NodeInfo{nodeV6}, Values: []dht.NodeAddr{valueA},
				Token: &token, BFpe: patternPeers, BFsd: patternSeeders,
				Bep51Return: dht.Bep51Return{Samples: &ignoredSamples},
			},
		},
		{
			id: "sample_query_error_identity_and_zero_result", operation: "sample_infohashes",
			transactionID: "SC", localID: localID,
			remote: netip.MustParseAddrPort("192.0.2.32:6912"), target: target,
			response: peerSampleClientSampleReturn(
				responseID, dht.CompactInfohashes{peerSampleClientID(0x67)}, nil,
				peerSampleClientInt64(1), peerSampleClientInt64(60),
			),
			failure: "query_error",
		},
	}

	fixtures := make([]peerSampleClientFixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixtures = append(fixtures, runPeerSampleClientScenario(t, scenario))
	}
	reconcilePeerSampleClientFixtures(t, fixtures)
}

func runPeerSampleClientScenario(
	t *testing.T,
	scenario peerSampleClientScenario,
) peerSampleClientFixture {
	t.Helper()
	sentinel := errors.New("peer/sample client oracle sentinel")
	scripted := &peerSampleClientScriptedServer{
		transactionID: scenario.transactionID,
		response: dht.RecvMsg{
			From: scenario.remote,
			Msg: dht.Msg{
				T: scenario.transactionID, Y: dht.YResponse, R: &scenario.response,
			},
		},
	}
	switch scenario.failure {
	case "":
	case "query_error":
		scripted.queryErr = sentinel
	case "pre_cancelled":
	case "typed_nil_error":
		var typedNil *dht.Error
		scripted.queryErr = typedNil
	default:
		t.Fatalf("%s: unknown failure %q", scenario.id, scenario.failure)
	}

	adapter := serverAdapter{nodeID: scenario.localID, server: scripted}
	ctx := context.Background()
	if scenario.failure == "pre_cancelled" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	}

	var result peerSampleClientResult
	var resultWasZero bool
	var err error
	switch scenario.operation {
	case "get_peers":
		var actual GetPeersResult
		actual, err = adapter.GetPeers(ctx, scenario.remote, scenario.infoHash)
		result = peerSampleClientProjectGetPeersResult(actual)
		resultWasZero = reflect.DeepEqual(actual, GetPeersResult{})
	case "get_peers_scrape":
		var actual GetPeersScrapeResult
		actual, err = adapter.GetPeersScrape(ctx, scenario.remote, scenario.infoHash)
		result = peerSampleClientProjectScrapeResult(t, actual)
		resultWasZero = reflect.DeepEqual(actual, GetPeersScrapeResult{})
	case "sample_infohashes":
		var actual SampleInfoHashesResult
		actual, err = adapter.SampleInfoHashes(ctx, scenario.remote, scenario.target)
		result = peerSampleClientProjectSampleResult(actual)
		resultWasZero = reflect.DeepEqual(actual, SampleInfoHashesResult{})
	default:
		t.Fatalf("%s: unknown operation %q", scenario.id, scenario.operation)
	}

	if len(scripted.queries) != 1 {
		t.Fatalf("%s: expected one Query call, got %d", scenario.id, len(scripted.queries))
	}
	captured := scripted.queries[0]
	expectedMethod, expectedArgs := peerSampleClientExpectedQuery(scenario)
	if captured.remote != scenario.remote || captured.method != expectedMethod ||
		!reflect.DeepEqual(captured.args, expectedArgs) {
		t.Fatalf(
			"%s: exact query changed: remote=%s method=%q args=%#v, want remote=%s method=%q args=%#v",
			scenario.id, captured.remote, captured.method, captured.args,
			scenario.remote, expectedMethod, expectedArgs,
		)
	}
	peerSampleClientAssertCanonicalQuery(
		t, scenario.id, scenario.transactionID, captured, expectedArgs,
	)

	outcome, errorText, identityPreserved, errorIsTypedNil :=
		peerSampleClientAssertOutcome(t, scenario, err, sentinel, resultWasZero)
	if scenario.operation == "get_peers_scrape" && err == nil {
		peerSampleClientAssertBloomDirection(t, scenario, result)
	}

	input := peerSampleClientInput{
		Operation: scenario.operation, TransactionIDHex: hex.EncodeToString([]byte(scenario.transactionID)),
		LocalID: scenario.localID.String(), Remote: peerSampleClientProjectAddr(scenario.remote),
		Response: peerSampleClientProjectResponse(scenario.response), Failure: scenario.failure,
	}
	if scenario.operation == "get_peers" || scenario.operation == "get_peers_scrape" {
		value := scenario.infoHash.String()
		input.InfoHash = &value
	} else {
		value := scenario.target.String()
		input.Target = &value
	}

	return peerSampleClientFixture{
		ID: scenario.id, Subsystem: peerSampleClientSubsystem,
		Runtime: peerSampleClientRuntime{IntBits: strconv.IntSize},
		Input:   input,
		Expected: peerSampleClientExpected{
			QueryCalls: 1, QueryMethod: captured.method,
			QueryRemote:  peerSampleClientProjectAddr(captured.remote),
			QueryArgs:    peerSampleClientProjectQueryArgs(captured.args),
			QueryWireHex: hex.EncodeToString(captured.wire), Outcome: outcome,
			ErrorText: errorText, ErrorIdentityPreserved: identityPreserved,
			ErrorIsTypedNil: errorIsTypedNil, ResultWasZero: resultWasZero, Result: result,
		},
	}
}

func peerSampleClientExpectedQuery(scenario peerSampleClientScenario) (string, dht.MsgArgs) {
	args := dht.MsgArgs{ID: scenario.localID}
	switch scenario.operation {
	case "get_peers":
		args.InfoHash = scenario.infoHash
		return dht.QGetPeers, args
	case "get_peers_scrape":
		args.InfoHash = scenario.infoHash
		args.Scrape = 1
		return dht.QGetPeers, args
	case "sample_infohashes":
		args.Target = scenario.target
		return dht.QSampleInfohashes, args
	default:
		panic("unreachable operation")
	}
}

func peerSampleClientAssertCanonicalQuery(
	t *testing.T,
	id string,
	expectedTransactionID string,
	captured peerSampleClientCapturedQuery,
	expectedArgs dht.MsgArgs,
) {
	t.Helper()
	var decoded dht.Msg
	if err := bencode.Unmarshal(captured.wire, &decoded); err != nil {
		t.Fatalf("%s: decode captured query: %v", id, err)
	}
	if decoded.T != expectedTransactionID ||
		decoded.Y != dht.YQuery || decoded.Q != captured.method || decoded.A == nil ||
		!reflect.DeepEqual(*decoded.A, expectedArgs) {
		t.Fatalf("%s: captured query wire does not decode to exact query", id)
	}
	canonical, err := bencode.Marshal(decoded)
	if err != nil {
		t.Fatalf("%s: re-encode captured query: %v", id, err)
	}
	if !bytes.Equal(canonical, captured.wire) {
		t.Fatalf("%s: captured query wire is not canonical", id)
	}
	if bytes.Contains(captured.wire, []byte("4:want")) ||
		bytes.Contains(captured.wire, []byte("6:noseed")) {
		t.Fatalf("%s: zero want/noseed unexpectedly encoded", id)
	}
}

func peerSampleClientAssertOutcome(
	t *testing.T,
	scenario peerSampleClientScenario,
	err error,
	sentinel error,
	resultWasZero bool,
) (outcome, errorText string, identityPreserved, errorIsTypedNil bool) {
	t.Helper()
	switch scenario.failure {
	case "query_error":
		if err != sentinel || !resultWasZero {
			t.Fatalf("%s: query error identity/result-zero contract changed", scenario.id)
		}
		return "query_error", sentinel.Error(), true, false
	case "pre_cancelled":
		if !errors.Is(err, context.Canceled) || !resultWasZero {
			t.Fatalf("%s: pre-cancel/result-zero contract changed", scenario.id)
		}
		return "context_cancelled", context.Canceled.Error(), false, false
	case "typed_nil_error":
		if err == nil || reflect.TypeOf(err).Kind() != reflect.Pointer ||
			!reflect.ValueOf(err).IsNil() || !resultWasZero {
			t.Fatalf("%s: typed-nil/result-zero contract changed", scenario.id)
		}
		return "typed_nil_error", "", false, true
	}

	missingBloom := scenario.operation == "get_peers_scrape" &&
		(scenario.response.BFpe == nil || scenario.response.BFsd == nil)
	if missingBloom {
		const message = "missing bloom filter in scrape response"
		if err == nil || err.Error() != message || !resultWasZero {
			t.Fatalf("%s: missing-bloom error/result-zero contract changed: %v", scenario.id, err)
		}
		return "missing_scrape_bloom", message, false, false
	}
	if err != nil {
		t.Fatalf("%s: unexpected adapter error: %v", scenario.id, err)
	}
	return "success", "", false, false
}

func peerSampleClientAssertBloomDirection(
	t *testing.T,
	scenario peerSampleClientScenario,
	result peerSampleClientResult,
) {
	t.Helper()
	if result.PeersBloom == nil || result.SeedersBloom == nil {
		t.Fatalf("%s: successful scrape omitted projected bloom", scenario.id)
	}
	peersHex := hex.EncodeToString(scenario.response.BFpe[:])
	seedersHex := hex.EncodeToString(scenario.response.BFsd[:])
	if result.PeersBloom.BloomHex != peersHex || result.SeedersBloom.BloomHex != seedersHex {
		t.Fatalf("%s: peer/seeder bloom direction changed", scenario.id)
	}
}

func peerSampleClientProjectResponse(value dht.Return) peerSampleClientResponseInput {
	return peerSampleClientResponseInput{
		ID:             value.ID.String(),
		NodesPresence:  peerSampleClientSlicePresence(value.Nodes),
		Nodes:          peerSampleClientProjectDHTNodes(value.Nodes),
		Nodes6Presence: peerSampleClientSlicePresence(value.Nodes6),
		Nodes6:         peerSampleClientProjectDHTNodes6(value.Nodes6),
		ValuesPresence: peerSampleClientSlicePresence(value.Values),
		Values:         peerSampleClientProjectDHTAddrs(value.Values),
		TokenPresence:  peerSampleClientPointerPresence(value.Token), TokenHex: peerSampleClientTokenHex(value.Token),
		SamplesPresence: peerSampleClientSamplesPresence(value.Samples),
		Samples:         peerSampleClientProjectSamplePointer(value.Samples),
		NumPresence:     peerSampleClientPointerPresence(value.Num), Num: peerSampleClientInt64Value(value.Num),
		IntervalPresence:     peerSampleClientPointerPresence(value.Interval),
		Interval:             peerSampleClientInt64Value(value.Interval),
		PeersBloomPresence:   peerSampleClientPointerPresence(value.BFpe),
		PeersBloomHex:        peerSampleClientDHTBloomHex(value.BFpe),
		SeedersBloomPresence: peerSampleClientPointerPresence(value.BFsd),
		SeedersBloomHex:      peerSampleClientDHTBloomHex(value.BFsd),
	}
}

func peerSampleClientProjectQueryArgs(value dht.MsgArgs) peerSampleClientQueryArgs {
	want := make([]string, 0, len(value.Want))
	for _, item := range value.Want {
		want = append(want, string(item))
	}
	return peerSampleClientQueryArgs{
		ID: value.ID.String(), InfoHash: value.InfoHash.String(), Target: value.Target.String(),
		TokenHex:     hex.EncodeToString([]byte(value.Token)),
		PortPresence: peerSampleClientPointerPresence(value.Port), ImpliedPort: value.ImpliedPort,
		WantPresence: peerSampleClientSlicePresence(value.Want), Want: want,
		NoSeed: value.NoSeed, Scrape: value.Scrape,
		BEP44FieldsAreZero: value.V == nil && value.Seq == nil && value.Cas == 0 &&
			value.K == [32]byte{} && value.Salt == nil && value.Sig == [64]byte{},
	}
}

func peerSampleClientProjectGetPeersResult(value GetPeersResult) peerSampleClientResult {
	return peerSampleClientResult{
		ID:             value.ID.String(),
		NodesPresence:  peerSampleClientSlicePresence(value.Nodes),
		Nodes:          peerSampleClientProjectClientNodes(value.Nodes),
		ValuesPresence: peerSampleClientSlicePresence(value.Values),
		Values:         peerSampleClientProjectAddrs(value.Values),
	}
}

func peerSampleClientProjectScrapeResult(
	t *testing.T,
	value GetPeersScrapeResult,
) peerSampleClientResult {
	result := peerSampleClientResult{
		ID:             value.ID.String(),
		NodesPresence:  peerSampleClientSlicePresence(value.Nodes),
		Nodes:          peerSampleClientProjectClientNodes(value.Nodes),
		ValuesPresence: peerSampleClientSlicePresence(value.Values),
		Values:         peerSampleClientProjectAddrs(value.Values),
	}
	if !reflect.DeepEqual(value, GetPeersScrapeResult{}) {
		result.PeersBloom = peerSampleClientDescribeBloom(t, &value.BfPeers)
		result.SeedersBloom = peerSampleClientDescribeBloom(t, &value.BfSeeders)
	}
	return result
}

func peerSampleClientProjectSampleResult(value SampleInfoHashesResult) peerSampleClientResult {
	return peerSampleClientResult{
		ID:              value.ID.String(),
		NodesPresence:   peerSampleClientSlicePresence(value.Nodes),
		Nodes:           peerSampleClientProjectClientNodes(value.Nodes),
		SamplesPresence: peerSampleClientSlicePresence(value.Samples),
		Samples:         peerSampleClientProjectIDs(value.Samples),
		Num:             value.Num, Interval: value.Interval,
	}
}

func peerSampleClientDescribeBloom(
	t *testing.T,
	filter *bloom.BloomFilter,
) *peerSampleClientBloomResult {
	t.Helper()
	words := filter.BitSet().Words()
	raw := make([]byte, len(words)*8)
	for index, word := range words {
		binary.BigEndian.PutUint64(raw[index*8:], word)
	}
	if len(raw) != 256 {
		t.Fatalf("adapter bloom has %d bytes, want 256", len(raw))
	}
	return &peerSampleClientBloomResult{
		BloomHex: hex.EncodeToString(raw), Capacity: filter.Cap(), Hashes: filter.K(),
		ApproximatedSize: filter.ApproximatedSize(),
	}
}

func peerSampleClientSampleReturn(
	id protocol.ID,
	samples dht.CompactInfohashes,
	nodes []dht.NodeInfo,
	num *int64,
	interval *int64,
) dht.Return {
	var samplesPointer *dht.CompactInfohashes
	if samples != nil {
		copy := append(dht.CompactInfohashes(nil), samples...)
		if len(samples) == 0 {
			copy = dht.CompactInfohashes{}
		}
		samplesPointer = &copy
	}
	var compactNodes dht.CompactIPv4NodeInfo
	if nodes != nil {
		compactNodes = append(dht.CompactIPv4NodeInfo(nil), nodes...)
		if len(nodes) == 0 {
			compactNodes = dht.CompactIPv4NodeInfo{}
		}
	}
	return dht.Return{
		ID: id, Nodes: compactNodes,
		Bep51Return: dht.Bep51Return{Samples: samplesPointer, Num: num, Interval: interval},
	}
}

func peerSampleClientPatternBloom(multiplier, add int) *dht.ScrapeBloomFilter {
	filter := new(dht.ScrapeBloomFilter)
	for index := range filter {
		filter[index] = byte((index*multiplier + add) & 0xff)
	}
	return filter
}

func peerSampleClientInt64(value int64) *int64 {
	return &value
}

func peerSampleClientID(last byte) protocol.ID {
	var value protocol.ID
	value[19] = last
	return value
}

func peerSampleClientNodeInfo(last byte, ip string, port uint16) dht.NodeInfo {
	return dht.NodeInfo{ID: peerSampleClientID(last), Addr: peerSampleClientNodeAddr(ip, port)}
}

func peerSampleClientNodeAddr(ip string, port uint16) dht.NodeAddr {
	addr := netip.MustParseAddr(ip)
	return dht.NodeAddr{IP: net.IP(addr.AsSlice()), Port: int(port)}
}

func peerSampleClientProjectDHTNodes(values dht.CompactIPv4NodeInfo) []peerSampleClientNode {
	if values == nil {
		return nil
	}
	result := make([]peerSampleClientNode, 0, len(values))
	for _, value := range values {
		result = append(result, peerSampleClientProjectDHTNode(value))
	}
	return result
}

func peerSampleClientProjectDHTNodes6(values dht.CompactIPv6NodeInfo) []peerSampleClientNode {
	if values == nil {
		return nil
	}
	result := make([]peerSampleClientNode, 0, len(values))
	for _, value := range values {
		result = append(result, peerSampleClientProjectDHTNode(value))
	}
	return result
}

func peerSampleClientProjectDHTNode(value dht.NodeInfo) peerSampleClientNode {
	return peerSampleClientNode{
		ID: value.ID.String(), Addr: peerSampleClientProjectAddr(value.Addr.ToAddrPort()),
	}
}

func peerSampleClientProjectClientNodes(values []NodeInfo) []peerSampleClientNode {
	if values == nil {
		return nil
	}
	result := make([]peerSampleClientNode, 0, len(values))
	for _, value := range values {
		result = append(result, peerSampleClientNode{
			ID: value.ID.String(), Addr: peerSampleClientProjectAddr(value.Addr),
		})
	}
	return result
}

func peerSampleClientProjectDHTAddrs(values []dht.NodeAddr) []peerSampleClientAddr {
	if values == nil {
		return nil
	}
	result := make([]peerSampleClientAddr, 0, len(values))
	for _, value := range values {
		result = append(result, peerSampleClientProjectAddr(value.ToAddrPort()))
	}
	return result
}

func peerSampleClientProjectAddrs(values []netip.AddrPort) []peerSampleClientAddr {
	if values == nil {
		return nil
	}
	result := make([]peerSampleClientAddr, 0, len(values))
	for _, value := range values {
		result = append(result, peerSampleClientProjectAddr(value))
	}
	return result
}

func peerSampleClientProjectAddr(value netip.AddrPort) peerSampleClientAddr {
	scope := uint32(0)
	if value.Addr().Zone() != "" {
		parsed, err := strconv.ParseUint(value.Addr().Zone(), 10, 32)
		if err != nil {
			panic(err)
		}
		scope = uint32(parsed)
	}
	return peerSampleClientAddr{
		IP: value.Addr().WithZone("").String(), Port: value.Port(), Scope: scope,
	}
}

func peerSampleClientProjectIDs(values []protocol.ID) []string {
	if values == nil {
		return nil
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value.String())
	}
	return result
}

func peerSampleClientProjectSamplePointer(values *dht.CompactInfohashes) []string {
	if values == nil {
		return nil
	}
	return peerSampleClientProjectIDs(*values)
}

func peerSampleClientTokenHex(value *string) string {
	if value == nil {
		return ""
	}
	return hex.EncodeToString([]byte(*value))
}

func peerSampleClientDHTBloomHex(value *dht.ScrapeBloomFilter) string {
	if value == nil {
		return ""
	}
	return hex.EncodeToString(value[:])
}

func peerSampleClientInt64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func peerSampleClientSlicePresence[T any](value []T) string {
	if value == nil {
		return "nil"
	}
	if len(value) == 0 {
		return "empty"
	}
	return "present"
}

func peerSampleClientSamplesPresence(value *dht.CompactInfohashes) string {
	if value == nil {
		return "nil"
	}
	if len(*value) == 0 {
		return "empty"
	}
	return "present"
}

func peerSampleClientPointerPresence[T any](value *T) string {
	if value == nil {
		return "nil"
	}
	return "present"
}

func reconcilePeerSampleClientFixtures(t *testing.T, fixtures []peerSampleClientFixture) {
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
		filepath.Dir(source), "../../../../testdata/parity/dht/peer_sample_client.jsonl",
	))
	if *updateDHTPeerSampleClientParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-peer-sample-client-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("peer/sample client fixture is stale; rerun with -update-dht-peer-sample-client-parity")
	}
}
