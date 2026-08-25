package dhtcrawler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/bloom"
	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	dhtwire "github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

var updateDHTCrawlerScrapeParity = flag.Bool(
	"update-dht-crawler-scrape-parity",
	false,
	"rewrite the Rust DHT crawler scrape parity fixture",
)

const crawlerScrapeFixtureSHA256 = "d434306fd60678be95cabd53d59ea152f6a013bf2e486f4bb2456aa8da2c6d9b"

var crawlerScrapeFixtureIDs = [...]string{
	"production_source_factory_and_lifecycle_contract",
	"scrape_error_drops_request_ip_and_preserves_cause",
	"success_present_empty_filters_ignores_values_and_hands_off_raw_blooms",
	"success_preserves_node_order_and_bloom_direction_before_persist",
	"cancelled_before_client_return_still_puts_responder_but_abandons_fanout_and_persist",
	"cancel_after_one_discovery_retains_prefix_but_abandons_suffix_and_persist",
	"cancellation_when_persist_send_is_unavailable_keeps_table_and_discovery_prefix",
	"lane_error_is_swallowed",
}

var crawlerScrapeExpectedNormalizedASTSHA256 = map[string]string{
	"batching.In":                           "f5ef939724dc08bc0fa39e9fa2e0863e45acd1c965609ad91fa7082fd6632b21",
	"batching.NewBatchingChannel":           "2c9a3fa894f82680a8cb8437d8dbad6d3bc2da9a7594c83553ef7650dd472dc6",
	"batching.Out":                          "f677733fd65c621331747365d30bc29503cda90a21e5aba68ece706afd5d2e3c",
	"bloom.FromScrape":                      "7298c86e1af2c667f8ae43775229426e70574a33dd4148ea2a71888bfe66f20b",
	"buffered.In":                           "47b8d0cda8a3039f6d0ea101430404511705d63aafe3ea9edf95e7883f17bedb",
	"buffered.NewBufferedConcurrentChannel": "562428750b1aaf7a4811758daa63468461d995ac00f36e4d7b620fedfb4633ec",
	"buffered.Run":                          "0a8f90020ab24fb50cad498fcf376777cde3b5f6aa6424da3e66b15b54e3292f",
	"client.GetPeersScrapeResult":           "29ab4bacfa43d6fcf24bae657383eb602540d7c7e4f0383981d093fc4b1491bb",
	"client.serverAdapter.GetPeersScrape":   "8c51361928643a78fd8e53b47d27e856e95d793a9a979212bec1eaec7544e3de",
	"config.NewDefaultConfig":               "d044a4710817daf9a87dfab03ce22f138da3c6e1bf94d40bbbfd0fea70673f32",
	"crawler.infoHashWithScrape":            "c9f4fdef915a61322eeaab348afd5896744000a5382416f474de44f21a6f835c",
	"crawler.nodeHasPeersForHash":           "1e2206b038dd5c1b70dff5a29cdf044ad7133b4876db75723081ab37c3d3da58",
	"crawler.start":                         "d61a318ce626352ee4f5cd5dd48191d767bbfe45b6a9def673cd185eada4f67b",
	"dht.ScrapeBloomFilter.ToBloomFilter":   "e059407f4ec58d9dced133d4add48bf41ed499fa15546d270ac17a882148608b",
	"discovery.NewDiscoveredNodes":          "8fcfcd3864cc5e815edbc40e3dd96393bddeb97ccf7c8eaa7fb30c7ad6382a17",
	"factory.New":                           "0204a00fd63b275339d63d622865858571c153bc81fc738784a78e1c150fec80",
	"ktable.DropAddr":                       "ab8ca0a52e22a72b0e37325cbccccf98de5211fc415e0ae139015ccdc9e91cd3",
	"ktable.NodeResponded":                  "52c5c68a8e6125a6d89839181e4dcb69bd62a1c857d2cf33c2f935d9c521e3d4",
	"ktable.PutNode":                        "f85a3fc30b4e45d98dadc9b26ff08b34a49e97d01757e4aa8d69757b0cacdc00",
	"scrape.requestScrape":                  "02c49474b9674a45d43e3b184e778ddd91abcd9db37239c134a7c26974efe1be",
	"scrape.runScrape":                      "04ce2add767cc7d213a74aa0aef46409abbaaa622ad4f7d1c21cef9df6b84e97",
}

type crawlerScrapeFixture struct {
	ID             string                `json:"id"`
	Subsystem      string                `json:"subsystem"`
	Classification string                `json:"classification"`
	Oracle         crawlerScrapeOracle   `json:"oracle"`
	Input          crawlerScrapeInput    `json:"input"`
	Expected       crawlerScrapeExpected `json:"expected"`
}

type crawlerScrapeOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Lane        string `json:"lane"`
	Client      string `json:"client"`
	Table       string `json:"table"`
	Discovery   string `json:"discovery"`
	Handoff     string `json:"handoff"`
	Clock       string `json:"clock"`
}

type crawlerScrapeInput struct {
	Kind                     string                    `json:"kind"`
	Requests                 []crawlerScrapeRequest    `json:"requests"`
	Outcomes                 []crawlerScrapeOutcome    `json:"outcomes"`
	TableSetup               []crawlerScrapeTableSetup `json:"tableSetup"`
	DiscoveryMode            string                    `json:"discoveryMode,omitempty"`
	DiscoveryCapacity        int                       `json:"discoveryCapacity"`
	CancelBeforeClientReturn bool                      `json:"cancelBeforeClientReturn"`
	CancelAfterDiscoveries   int                       `json:"cancelAfterDiscoveries"`
	HandoffMode              string                    `json:"handoffMode,omitempty"`
	HandoffCapacity          int                       `json:"handoffCapacity"`
	CancelAtHandoffInCall    int                       `json:"cancelAtHandoffInCall"`
	LaneReturnError          bool                      `json:"laneReturnError"`
}

type crawlerScrapeRequest struct {
	InfoHash string               `json:"infoHash"`
	Node     crawlerScrapeAddress `json:"node"`
}

type crawlerScrapeOutcome struct {
	Kind            string                 `json:"kind"`
	ResponseID      string                 `json:"responseId"`
	Values          []crawlerScrapeAddress `json:"values"`
	Nodes           []crawlerScrapeNode    `json:"nodes"`
	PeersBloomHex   string                 `json:"peersBloomHex"`
	SeedersBloomHex string                 `json:"seedersBloomHex"`
}

type crawlerScrapeNode struct {
	ID   string               `json:"id"`
	Addr crawlerScrapeAddress `json:"addr"`
}

type crawlerScrapeAddress struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope"`
}

type crawlerScrapeIP struct {
	IP    string `json:"ip"`
	Scope uint32 `json:"scope"`
}

type crawlerScrapeTableSetup struct {
	Kind string               `json:"kind"`
	ID   string               `json:"id"`
	Addr crawlerScrapeAddress `json:"addr"`
}

type crawlerScrapeExpected struct {
	ClientCalls       []crawlerScrapeClientCall `json:"clientCalls"`
	SameContext       bool                      `json:"sameContext"`
	BatchCalls        int                       `json:"batchCalls"`
	Commands          []crawlerScrapeCommand    `json:"commands"`
	DiscoveryInCalls  int                       `json:"discoveryInCalls"`
	Discoveries       []crawlerScrapeNode       `json:"discoveries"`
	HandoffInCalls    int                       `json:"handoffInCalls"`
	HandoffDeliveries []crawlerScrapeHandoff    `json:"handoffDeliveries"`
	Events            []string                  `json:"events"`
	TablePost         crawlerScrapeTablePost    `json:"tablePost"`
	RunReturned       bool                      `json:"runReturned"`
	ContextCancelled  bool                      `json:"contextCancelled"`
	CallbackCompleted bool                      `json:"callbackCompleted"`
	Source            *crawlerScrapeSource      `json:"source,omitempty"`
}

type crawlerScrapeClientCall struct {
	Node     crawlerScrapeAddress `json:"node"`
	InfoHash string               `json:"infoHash"`
}

type crawlerScrapeCommand struct {
	Kind                   string                `json:"kind"`
	ID                     string                `json:"id,omitempty"`
	Addr                   *crawlerScrapeAddress `json:"addr,omitempty"`
	DropIP                 *crawlerScrapeIP      `json:"dropIp,omitempty"`
	OptionCount            int                   `json:"optionCount"`
	Reason                 string                `json:"reason,omitempty"`
	ErrorIdentityPreserved bool                  `json:"errorIdentityPreserved"`
	StoredResponded        bool                  `json:"storedResponded"`
}

type crawlerScrapeBloom struct {
	BloomHex string `json:"bloomHex"`
}

type crawlerScrapeHandoff struct {
	InfoHash     string               `json:"infoHash"`
	Node         crawlerScrapeAddress `json:"node"`
	SeedersBloom crawlerScrapeBloom   `json:"seedersBloom"`
	PeersBloom   crawlerScrapeBloom   `json:"peersBloom"`
}

type crawlerScrapeTablePost struct {
	Nodes []crawlerScrapeNodePost `json:"nodes"`
}

type crawlerScrapeNodePost struct {
	ID              string                `json:"id"`
	Present         bool                  `json:"present"`
	Addr            *crawlerScrapeAddress `json:"addr,omitempty"`
	Responded       bool                  `json:"responded"`
	RetainedDropped bool                  `json:"retainedDropped"`
}

type crawlerScrapeSource struct {
	RunErrorIgnored                        bool                `json:"runErrorIgnored"`
	SharedCallbackContext                  bool                `json:"sharedCallbackContext"`
	ErrorDropsRequestIPAndScopeWithoutPort bool                `json:"errorDropsRequestIpAndScopeWithoutPort"`
	ErrorReason                            string              `json:"errorReason"`
	ErrorReasonWrapsCause                  bool                `json:"errorReasonWrapsCause"`
	SuccessUsesResponseID                  bool                `json:"successUsesResponseId"`
	SuccessUsesRequestAddress              bool                `json:"successUsesRequestAddress"`
	SuccessUsesNodeRespondedOption         bool                `json:"successUsesNodeRespondedOption"`
	NoPostClientCancellationBeforePutNode  bool                `json:"noPostClientCancellationBeforePutNode"`
	ResponseValuesIgnored                  bool                `json:"responseValuesIgnored"`
	DiscoveryTimeoutMS                     int                 `json:"discoveryTimeoutMs"`
	DiscoveryUsesResponseOrder             bool                `json:"discoveryUsesResponseOrder"`
	DiscoveryCancelBreakLabelled           bool                `json:"discoveryCancelBreakLabelled"`
	DiscoveryCancelBreakScope              string              `json:"discoveryCancelBreakScope"`
	DiscoveryCancellationRetainsPrefix     bool                `json:"discoveryCancellationRetainsPrefix"`
	DiscoveryCancellationScansSuffix       bool                `json:"discoveryCancellationScansSuffix"`
	DiscoveryInAccessorEvaluatedForSuffix  bool                `json:"discoveryInAccessorEvaluatedForSuffix"`
	RawBloomDirectionPreserved             bool                `json:"rawBloomDirectionPreserved"`
	HandoffUsesOriginalRequest             bool                `json:"handoffUsesOriginalRequest"`
	HandoffAfterDiscovery                  bool                `json:"handoffAfterDiscovery"`
	HandoffCancellationRetainsTable        bool                `json:"handoffCancellationRetainsTable"`
	RunPersistSourcesExecuted              bool                `json:"runPersistSourcesExecuted"`
	ProductionScrapeCapacity               int                 `json:"productionScrapeCapacity"`
	ProductionScrapeConcurrency            int                 `json:"productionScrapeConcurrency"`
	ProductionHandoffCapacity              int                 `json:"productionHandoffCapacity"`
	ProductionHandoffMaxBatchSize          int                 `json:"productionHandoffMaxBatchSize"`
	ProductionHandoffIntervalMS            int                 `json:"productionHandoffIntervalMs"`
	ProductionHandoffOutputCapacity        int                 `json:"productionHandoffOutputCapacity"`
	DefaultScalingFactor                   int                 `json:"defaultScalingFactor"`
	ConsumerDequeuesBeforeSemaphore        bool                `json:"consumerDequeuesBeforeSemaphore"`
	ConsumerCallbacksDetached              bool                `json:"consumerCallbacksDetached"`
	ConsumerCallbacksJoined                bool                `json:"consumerCallbacksJoined"`
	MaximumRetainedWork                    string              `json:"maximumRetainedWork"`
	ClosedInputChecksOpenBoolean           bool                `json:"closedInputChecksOpenBoolean"`
	ClosedInputOutcome                     string              `json:"closedInputOutcome"`
	ProductionDiscoveryCapacity            int                 `json:"productionDiscoveryCapacity"`
	ProductionDiscoveryMaxBatchSize        int                 `json:"productionDiscoveryMaxBatchSize"`
	ProductionDiscoveryIntervalMS          int                 `json:"productionDiscoveryIntervalMs"`
	ProductionDiscoveryOutputCapacity      int                 `json:"productionDiscoveryOutputCapacity"`
	StartLaunchesWorkerDetached            bool                `json:"startLaunchesWorkerDetached"`
	StartWaitsOnlyStopped                  bool                `json:"startWaitsOnlyStopped"`
	StartDefersSharedContextCancel         bool                `json:"startDefersSharedContextCancel"`
	StartJoinsWorkerOrCallbacks            bool                `json:"startJoinsWorkerOrCallbacks"`
	NormalizedASTSHA256                    map[string]string   `json:"normalizedAstSha256"`
	PrerequisiteFixtureSHA256              map[string]string   `json:"prerequisiteFixtureSha256"`
	EvidenceCommit                         map[string]string   `json:"evidenceCommit"`
	SourceSHA256                           map[string]string   `json:"sourceSha256"`
	ModuleLines                            map[string][]string `json:"moduleLines"`
	Nonclaims                              []string            `json:"nonclaims"`
	Evidence                               string              `json:"evidence"`
}

type crawlerScrapeEventLog struct {
	mutex  sync.Mutex
	events []string
}

func (l *crawlerScrapeEventLog) append(event string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.events = append(l.events, event)
}

func (l *crawlerScrapeEventLog) snapshot() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return append([]string{}, l.events...)
}

type crawlerScrapeManualLane struct {
	requests  []nodeHasPeersForHash
	returnErr error
	events    *crawlerScrapeEventLog
	completed bool
}

func (*crawlerScrapeManualLane) In() chan<- nodeHasPeersForHash {
	panic("scrape worker must not request its input sender")
}

func (l *crawlerScrapeManualLane) Run(_ context.Context, callback func(nodeHasPeersForHash)) error {
	for index, request := range l.requests {
		l.events.append("lane_callback:" + strconv.Itoa(index+1))
		callback(request)
		l.completed = true
	}
	if l.returnErr != nil {
		l.events.append("lane_return_error")
	}
	return l.returnErr
}

type crawlerScrapeClient struct {
	client.Client
	wantContext        context.Context
	outcomes           []crawlerScrapeOutcome
	cancelBeforeReturn context.CancelFunc
	events             *crawlerScrapeEventLog
	calls              []crawlerScrapeClientCall
	sameContext        bool
}

var crawlerScrapeSentinel = errors.New("oracle scrape failure")

func (c *crawlerScrapeClient) GetPeersScrape(
	ctx context.Context,
	addr netip.AddrPort,
	infoHash protocol.ID,
) (client.GetPeersScrapeResult, error) {
	c.sameContext = c.sameContext && ctx == c.wantContext
	c.calls = append(c.calls, crawlerScrapeClientCall{
		Node: crawlerScrapeProjectAddress(addr), InfoHash: infoHash.String(),
	})
	c.events.append("client_get_peers_scrape:" + strconv.Itoa(len(c.calls)))
	outcome := c.outcomes[len(c.calls)-1]
	if c.cancelBeforeReturn != nil {
		c.cancelBeforeReturn()
		c.cancelBeforeReturn = nil
	}
	result := client.GetPeersScrapeResult{
		ID:        protocol.MustParseID(outcome.ResponseID),
		BfPeers:   crawlerScrapeFilter(outcome.PeersBloomHex),
		BfSeeders: crawlerScrapeFilter(outcome.SeedersBloomHex),
	}
	for _, value := range outcome.Values {
		result.Values = append(result.Values, crawlerScrapeAddr(value))
	}
	for _, node := range outcome.Nodes {
		result.Nodes = append(result.Nodes, client.NodeInfo{
			ID: protocol.MustParseID(node.ID), Addr: crawlerScrapeAddr(node.Addr),
		})
	}
	if outcome.Kind == "error" {
		return result, crawlerScrapeSentinel
	}
	return result, nil
}

type crawlerScrapeDiscovery struct {
	input      chan ktable.Node
	events     *crawlerScrapeEventLog
	mutex      sync.Mutex
	inCalls    int
	deliveries []ktable.Node
}

func (d *crawlerScrapeDiscovery) In() chan<- ktable.Node {
	d.mutex.Lock()
	d.inCalls++
	call := d.inCalls
	d.mutex.Unlock()
	d.events.append("discovery_in:" + strconv.Itoa(call))
	return d.input
}

func (*crawlerScrapeDiscovery) Out() <-chan []ktable.Node {
	panic("scrape worker must not request discovered-node output")
}

func (d *crawlerScrapeDiscovery) collect(node ktable.Node) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.deliveries = append(d.deliveries, node)
}

func (d *crawlerScrapeDiscovery) drainBuffered() {
	for len(d.input) > 0 {
		d.collect(<-d.input)
	}
}

func (d *crawlerScrapeDiscovery) snapshot() (int, []crawlerScrapeNode) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	nodes := make([]crawlerScrapeNode, 0, len(d.deliveries))
	for _, node := range d.deliveries {
		nodes = append(nodes, crawlerScrapeProjectNode(node.ID(), node.Addr()))
	}
	return d.inCalls, nodes
}

type crawlerScrapeHandoffLane struct {
	input          chan infoHashWithScrape
	events         *crawlerScrapeEventLog
	cancel         context.CancelFunc
	cancelAtInCall int
	mutex          sync.Mutex
	inCalls        int
}

func (l *crawlerScrapeHandoffLane) In() chan<- infoHashWithScrape {
	l.mutex.Lock()
	l.inCalls++
	call := l.inCalls
	l.mutex.Unlock()
	l.events.append("persist_sources_in:" + strconv.Itoa(call))
	if l.cancel != nil && call == l.cancelAtInCall {
		l.cancel()
	}
	return l.input
}

func (*crawlerScrapeHandoffLane) Out() <-chan []infoHashWithScrape {
	panic("scrape oracle must not execute runPersistSources")
}

func (l *crawlerScrapeHandoffLane) snapshot(t *testing.T) (int, []crawlerScrapeHandoff) {
	t.Helper()
	l.mutex.Lock()
	calls := l.inCalls
	l.mutex.Unlock()
	deliveries := make([]crawlerScrapeHandoff, 0, len(l.input))
	for len(l.input) > 0 {
		deliveries = append(deliveries, crawlerScrapeProjectHandoff(t, <-l.input))
	}
	return calls, deliveries
}

type crawlerScrapeTracingTable struct {
	ktable.Table
	sentinel   error
	events     *crawlerScrapeEventLog
	batchCalls int
	commands   []crawlerScrapeCommand
}

func (t *crawlerScrapeTracingTable) BatchCommand(commands ...ktable.Command) {
	t.batchCalls++
	start := len(t.commands)
	for _, raw := range commands {
		switch command := raw.(type) {
		case ktable.DropAddr:
			ip := crawlerScrapeProjectIP(command.Addr)
			t.commands = append(t.commands, crawlerScrapeCommand{
				Kind: "drop_addr", DropIP: &ip, Reason: command.Reason.Error(),
				ErrorIdentityPreserved: errors.Is(command.Reason, t.sentinel),
			})
			t.events.append("batch_drop_addr")
		case ktable.PutNode:
			addr := crawlerScrapeProjectAddress(command.Addr)
			t.commands = append(t.commands, crawlerScrapeCommand{
				Kind: "put_node", ID: command.ID.String(), Addr: &addr,
				OptionCount: len(command.Options),
			})
			t.events.append("batch_put_node")
		default:
			panic(fmt.Sprintf("unexpected scrape command %T", raw))
		}
	}
	t.Table.BatchCommand(commands...)
	for index := start; index < len(t.commands); index++ {
		command := &t.commands[index]
		if command.Kind == "put_node" {
			post := crawlerScrapeFindNode(t.Table, protocol.MustParseID(command.ID))
			command.StoredResponded = post.fixture.Present && post.fixture.Responded
		}
	}
}

type crawlerScrapeScenario struct {
	id                       string
	classification           string
	request                  *nodeHasPeersForHash
	outcome                  *crawlerScrapeOutcome
	seedErrorNode            bool
	discoveryMode            string
	discoveryCapacity        int
	cancelBeforeClientReturn bool
	cancelAfterDiscoveries   int
	handoffMode              string
	handoffCapacity          int
	cancelAtHandoffInCall    int
	laneReturnError          bool
}

func TestGenerateDHTCrawlerScrapeParity(t *testing.T) {
	empty := strings.Repeat("00", 256)
	seedersPattern := crawlerScrapePatternHex(net.IPv4(127, 0, 0, 1).To4())
	peersPattern := crawlerScrapePatternHex(net.ParseIP("2001:db8::1").To16())
	nodes := []crawlerScrapeNode{
		crawlerScrapeNodeValue(31, "203.0.113.31:7231"),
		crawlerScrapeNodeValue(32, "[2001:db8::32%9]:7232"),
	}
	orderedDuplicateNodes := []crawlerScrapeNode{nodes[0], nodes[1], nodes[0]}
	cancelNodes := []crawlerScrapeNode{
		crawlerScrapeNodeValue(41, "203.0.113.41:7241"),
		crawlerScrapeNodeValue(42, "203.0.113.42:7242"),
		crawlerScrapeNodeValue(43, "203.0.113.43:7243"),
	}
	fixtures := []crawlerScrapeFixture{crawlerScrapeSourceFixture(t)}
	fixtures = append(fixtures,
		crawlerScrapeRunScenario(t, crawlerScrapeScenario{
			id: "scrape_error_drops_request_ip_and_preserves_cause", classification: "RUNTIME_EXACT",
			request:       crawlerScrapeRequestValue(112, "[fe80::112%7]:7112"),
			outcome:       crawlerScrapeOutcomeValue("error", 212, nil, nodes, peersPattern, seedersPattern),
			seedErrorNode: true, discoveryMode: "unbuffered_no_receiver",
			handoffMode: "unbuffered_no_receiver",
		}),
		crawlerScrapeRunScenario(t, crawlerScrapeScenario{
			id:             "success_present_empty_filters_ignores_values_and_hands_off_raw_blooms",
			classification: "RUNTIME_EXACT", request: crawlerScrapeRequestValue(113, "198.51.100.113:7113"),
			outcome: crawlerScrapeOutcomeValue("success", 213,
				[]string{"203.0.113.80:7380", "203.0.113.80:7380"}, nil, empty, empty),
			discoveryMode: "unbuffered_no_receiver", handoffMode: "buffered_accept_one", handoffCapacity: 1,
		}),
		crawlerScrapeRunScenario(t, crawlerScrapeScenario{
			id:             "success_preserves_node_order_and_bloom_direction_before_persist",
			classification: "RUNTIME_EXACT", request: crawlerScrapeRequestValue(114, "198.51.100.114:7114"),
			outcome:       crawlerScrapeOutcomeValue("success", 214, nil, orderedDuplicateNodes, peersPattern, seedersPattern),
			discoveryMode: "buffered_accept_all", discoveryCapacity: len(orderedDuplicateNodes),
			handoffMode: "buffered_accept_one", handoffCapacity: 1,
		}),
		crawlerScrapeRunScenario(t, crawlerScrapeScenario{
			id:                       "cancelled_before_client_return_still_puts_responder_but_abandons_fanout_and_persist",
			classification:           "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
			request:                  crawlerScrapeRequestValue(115, "198.51.100.115:7115"),
			outcome:                  crawlerScrapeOutcomeValue("success", 215, nil, nodes, peersPattern, seedersPattern),
			cancelBeforeClientReturn: true, discoveryMode: "unbuffered_no_receiver",
			handoffMode: "unbuffered_no_receiver",
		}),
		crawlerScrapeRunScenario(t, crawlerScrapeScenario{
			id:             "cancel_after_one_discovery_retains_prefix_but_abandons_suffix_and_persist",
			classification: "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
			request:        crawlerScrapeRequestValue(116, "198.51.100.116:7116"),
			outcome:        crawlerScrapeOutcomeValue("success", 216, nil, cancelNodes, peersPattern, seedersPattern),
			discoveryMode:  "unbuffered_cancel_after_prefix", cancelAfterDiscoveries: 1,
			handoffMode: "unbuffered_no_receiver",
		}),
		crawlerScrapeRunScenario(t, crawlerScrapeScenario{
			id:             "cancellation_when_persist_send_is_unavailable_keeps_table_and_discovery_prefix",
			classification: "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
			request:        crawlerScrapeRequestValue(117, "198.51.100.117:7117"),
			outcome:        crawlerScrapeOutcomeValue("success", 217, nil, nodes, peersPattern, seedersPattern),
			discoveryMode:  "buffered_accept_all", discoveryCapacity: len(nodes),
			handoffMode: "unbuffered_cancel_at_in", cancelAtHandoffInCall: 1,
		}),
		crawlerScrapeRunScenario(t, crawlerScrapeScenario{
			id: "lane_error_is_swallowed", classification: "GO_ONLY_LANE",
			discoveryMode: "unbuffered_no_receiver", handoffMode: "unbuffered_no_receiver",
			laneReturnError: true,
		}),
	)

	wantClassifications := [...]string{
		"SOURCE_ONLY", "RUNTIME_EXACT", "RUNTIME_EXACT", "RUNTIME_EXACT",
		"RUNTIME_WITH_OWNED_SHUTDOWN_DELTA", "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
		"RUNTIME_WITH_OWNED_SHUTDOWN_DELTA", "GO_ONLY_LANE",
	}
	if len(fixtures) != len(crawlerScrapeFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerScrapeFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerScrapeFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerScrapeFixtureIDs[index])
		}
		if fixture.Subsystem != "dht_crawler_scrape" || fixture.Classification != wantClassifications[index] {
			t.Fatalf("fixture %s subsystem/classification = %q/%q", fixture.ID, fixture.Subsystem, fixture.Classification)
		}
	}
	crawlerScrapeReconcileFixtures(t, fixtures)
}

func crawlerScrapeSourceFixture(t *testing.T) crawlerScrapeFixture {
	t.Helper()
	config := NewDefaultConfig()
	return crawlerScrapeFixture{
		ID: crawlerScrapeFixtureIDs[0], Subsystem: "dht_crawler_scrape", Classification: "SOURCE_ONLY",
		Oracle: crawlerScrapeOracle{
			Composition: "production_source_factory_and_lifecycle_freshness_gate",
			Determinism: "exact_normalized_AST_source_module_and_prerequisite_fixture_SHA256",
			Lane:        "production_BufferedConcurrentChannel_source_shape", Client: "production_Client_GetPeersScrape_interface",
			Table: "production_KTable_command_and_query_source_shapes", Discovery: "production_BatchingChannel_source_shape",
			Handoff: "production_persistSources_BatchingChannel_input_shape_only", Clock: "timeout_and_NodeResponded_source_only",
		},
		Input: crawlerScrapeInput{
			Kind: "source_contract", Requests: []crawlerScrapeRequest{}, Outcomes: []crawlerScrapeOutcome{},
			TableSetup: []crawlerScrapeTableSetup{},
		},
		Expected: crawlerScrapeExpected{
			ClientCalls: []crawlerScrapeClientCall{}, Commands: []crawlerScrapeCommand{},
			Discoveries: []crawlerScrapeNode{}, HandoffDeliveries: []crawlerScrapeHandoff{}, Events: []string{},
			TablePost: crawlerScrapeTablePost{Nodes: []crawlerScrapeNodePost{}}, RunReturned: false,
			Source: &crawlerScrapeSource{
				RunErrorIgnored: true, SharedCallbackContext: true,
				ErrorDropsRequestIPAndScopeWithoutPort: true,
				ErrorReason:                            "failed to get peers from p: <cause>", ErrorReasonWrapsCause: true,
				SuccessUsesResponseID: true, SuccessUsesRequestAddress: true, SuccessUsesNodeRespondedOption: true,
				NoPostClientCancellationBeforePutNode: true, ResponseValuesIgnored: true,
				DiscoveryTimeoutMS: 1000, DiscoveryUsesResponseOrder: true,
				DiscoveryCancelBreakLabelled: false, DiscoveryCancelBreakScope: "select_only_not_for_loop",
				DiscoveryCancellationRetainsPrefix: true, DiscoveryCancellationScansSuffix: true,
				DiscoveryInAccessorEvaluatedForSuffix: true, RawBloomDirectionPreserved: true,
				HandoffUsesOriginalRequest: true, HandoffAfterDiscovery: true,
				HandoffCancellationRetainsTable: true, RunPersistSourcesExecuted: false,
				ProductionScrapeCapacity:    10 * int(config.ScalingFactor),
				ProductionScrapeConcurrency: 20 * int(config.ScalingFactor),
				ProductionHandoffCapacity:   1000, ProductionHandoffMaxBatchSize: 1000,
				ProductionHandoffIntervalMS: 60000, ProductionHandoffOutputCapacity: 1,
				DefaultScalingFactor: int(config.ScalingFactor), ConsumerDequeuesBeforeSemaphore: true,
				ConsumerCallbacksDetached: true, ConsumerCallbacksJoined: false,
				MaximumRetainedWork:             "capacity_plus_concurrency_plus_one_acquire_waiter",
				ClosedInputChecksOpenBoolean:    false,
				ClosedInputOutcome:              "repeated_zero_value_callbacks_can_issue_invalid_zero_request_work",
				ProductionDiscoveryCapacity:     100 * int(config.ScalingFactor),
				ProductionDiscoveryMaxBatchSize: 10, ProductionDiscoveryIntervalMS: 10,
				ProductionDiscoveryOutputCapacity: 1, StartLaunchesWorkerDetached: true,
				StartWaitsOnlyStopped: true, StartDefersSharedContextCancel: true,
				StartJoinsWorkerOrCallbacks: false,
				NormalizedASTSHA256:         crawlerScrapeNormalizedASTDigests(t),
				PrerequisiteFixtureSHA256:   crawlerScrapePrerequisiteDigests(t),
				EvidenceCommit: map[string]string{
					"peer_client_oracle":      "1f00b40705ba527721208023ddec64220fb40729",
					"scrape_bloom_oracle":     "b9b430637fa977316db5da138a75c106a9a355ce",
					"ktable_core_oracle":      "b345998fe0e3f3f99d35745588cbd8c375124ac8",
					"ktable_temporal_oracle":  "1df4d7a09f74e13e75ea2e1ab1dcfc67a130ed9d",
					"info_hash_triage_oracle": "6aece7ac7605507aaf5ccdcc9adf2497170b071d",
					"discovered_nodes_oracle": "069b3febcf1e270ffdaef9941bf56d494697bf2c",
					"typed_scrape_route":      "a5e2276ea9e2d93a75c3af8f4226bf2c333d27be",
					"scraped_source_route":    "a76591e92430ceb65fc7eb62af4ffbbaa791dad7",
					"get_peers_oracle":        "19f568e01c637a8ae1b94f38e3db2c9f95734d8c",
				},
				SourceSHA256: crawlerScrapeSourceDigests(t), ModuleLines: crawlerScrapeModuleLines(t),
				Nonclaims: []string{
					"exact_ready_select_tie_winner",
					"goroutine_callback_scheduling_completion_or_order",
					"semaphore_or_channel_fairness",
					"closed_buffered_input_runtime_execution",
					"callback_join_guarantee",
					"actual_one_second_timeout_elapsed_in_runtime_rows",
					"arbitrary_side_effects_of_eagerly_evaluated_channel_accessors_beyond_recorded_In_call_counts",
					"send_to_closed_Go_channel_behavior",
					"exact_wall_clock_NodeResponded_timestamp",
					"KTable_map_iteration_eviction_or_internal_layout",
					"opaque_NodeOption_function_identity",
					"Bloom_capacity_hash_count_set_bit_count_approximation_or_concurrent_mutation_after_handoff_runtime_assertions",
					"live_DNS_UDP_DHT_network_or_client_wire_behavior",
					"downstream_discovered_node_deduplication_scheduling_or_routing",
					"runPersistSources_batching_deduplication_model_conversion_or_database_behavior",
					"batching_ticker_schedule_log_or_metrics_delivery",
					"production_throughput_total_retention_or_waiter_fairness",
					"production_application_supervisor_deployment_or_readiness",
					"arbitrary_textual_IPv6_zones_runtime_rows_use_unscoped_or_numeric_scope_only",
					"Rust_public_API_owned_task_stats_or_shutdown_lifecycle_no_Rust_consumer_exists_in_this_slice",
				},
				Evidence: "runtime rows execute actual runScrape and requestScrape through controlled interfaces and an actual KTable; persistSources is observed only at its raw input and runPersistSources is never executed",
			},
		},
	}
}

func crawlerScrapeRunScenario(t *testing.T, scenario crawlerScrapeScenario) crawlerScrapeFixture {
	t.Helper()
	events := &crawlerScrapeEventLog{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manual := &crawlerScrapeManualLane{events: events}
	input := crawlerScrapeInput{
		Kind: "run_scrape", Requests: []crawlerScrapeRequest{}, Outcomes: []crawlerScrapeOutcome{},
		TableSetup: []crawlerScrapeTableSetup{}, DiscoveryMode: scenario.discoveryMode,
		DiscoveryCapacity: scenario.discoveryCapacity, CancelBeforeClientReturn: scenario.cancelBeforeClientReturn,
		CancelAfterDiscoveries: scenario.cancelAfterDiscoveries, HandoffMode: scenario.handoffMode,
		HandoffCapacity: scenario.handoffCapacity, CancelAtHandoffInCall: scenario.cancelAtHandoffInCall,
		LaneReturnError: scenario.laneReturnError,
	}
	if scenario.request != nil {
		manual.requests = []nodeHasPeersForHash{*scenario.request}
		input.Requests = []crawlerScrapeRequest{crawlerScrapeProjectRequest(*scenario.request)}
	}
	if scenario.outcome != nil {
		input.Outcomes = []crawlerScrapeOutcome{*scenario.outcome}
	}
	if scenario.laneReturnError {
		manual.returnErr = errors.New("oracle lane failure")
	}

	base := ktable.New(ktable.Params{NodeID: protocol.ID{}}).Table
	var retainedSeed ktable.Node
	var seedID protocol.ID
	if scenario.seedErrorNode {
		seedID = crawlerPingWorkerID(12)
		base.PutNode(seedID, scenario.request.node)
		retainedSeed = crawlerScrapeFindNode(base, seedID).node
		base.PutNode(seedID, scenario.request.node)
		input.TableSetup = []crawlerScrapeTableSetup{{
			Kind: "put_same_node_twice_to_populate_reverse_map", ID: seedID.String(),
			Addr: crawlerScrapeProjectAddress(scenario.request.node),
		}}
	}
	tracing := &crawlerScrapeTracingTable{Table: base, sentinel: crawlerScrapeSentinel, events: events}
	scripted := &crawlerScrapeClient{wantContext: ctx, sameContext: true, events: events}
	if scenario.outcome != nil {
		scripted.outcomes = []crawlerScrapeOutcome{*scenario.outcome}
	}
	if scenario.cancelBeforeClientReturn {
		scripted.cancelBeforeReturn = cancel
	}
	discovery := &crawlerScrapeDiscovery{input: make(chan ktable.Node, scenario.discoveryCapacity), events: events}
	var discoveryReceiverDone chan struct{}
	if scenario.cancelAfterDiscoveries > 0 {
		if scenario.cancelAfterDiscoveries != 1 {
			t.Fatal("controlled scrape discovery prefix only supports one delivery")
		}
		ready := make(chan struct{})
		discoveryReceiverDone = make(chan struct{})
		go func() {
			close(ready)
			discovery.collect(<-discovery.input)
			cancel()
			close(discoveryReceiverDone)
		}()
		<-ready
	}
	handoff := &crawlerScrapeHandoffLane{
		input: make(chan infoHashWithScrape, scenario.handoffCapacity), events: events,
		cancel: cancel, cancelAtInCall: scenario.cancelAtHandoffInCall,
	}
	c := crawler{
		scrape: manual, client: scripted, kTable: tracing,
		discoveredNodes: discovery, persistSources: handoff,
	}
	c.runScrape(ctx)
	if discoveryReceiverDone != nil {
		crawlerScrapeWait(t, discoveryReceiverDone, "discovery prefix receiver")
	}
	discovery.drainBuffered()
	discoveryInCalls, discoveries := discovery.snapshot()
	handoffInCalls, handoffDeliveries := handoff.snapshot(t)

	tablePost := crawlerScrapeTablePost{Nodes: []crawlerScrapeNodePost{}}
	if scenario.seedErrorNode {
		post := crawlerScrapeFindNode(base, seedID)
		post.fixture.RetainedDropped = retainedSeed != nil && retainedSeed.Dropped()
		tablePost.Nodes = append(tablePost.Nodes, post.fixture)
	}
	if scenario.outcome != nil && scenario.outcome.Kind == "success" {
		tablePost.Nodes = append(tablePost.Nodes,
			crawlerScrapeFindNode(base, protocol.MustParseID(scenario.outcome.ResponseID)).fixture)
	}
	return crawlerScrapeFixture{
		ID: scenario.id, Subsystem: "dht_crawler_scrape", Classification: scenario.classification,
		Oracle: crawlerScrapeOracle{
			Composition: "actual_runScrape_requestScrape_manual_callback_lane_scripted_client_actual_KTable",
			Determinism: "synchronous_callback_controlled_channel_acceptance_and_explicit_cancellation_gates",
			Lane:        "manual_in_order_callback_interface", Client: "scripted_Client_GetPeersScrape_override",
			Table: "tracing_wrapper_over_actual_KTable", Discovery: scenario.discoveryMode,
			Handoff: scenario.handoffMode + "_raw_persistSources_input_only", Clock: "NodeResponded_boolean_only_no_timestamp_assertion",
		},
		Input: input,
		Expected: crawlerScrapeExpected{
			ClientCalls: append([]crawlerScrapeClientCall{}, scripted.calls...), SameContext: scripted.sameContext,
			BatchCalls: tracing.batchCalls, Commands: append([]crawlerScrapeCommand{}, tracing.commands...),
			DiscoveryInCalls: discoveryInCalls, Discoveries: discoveries,
			HandoffInCalls: handoffInCalls, HandoffDeliveries: handoffDeliveries,
			Events: events.snapshot(), TablePost: tablePost, RunReturned: true,
			ContextCancelled: ctx.Err() != nil, CallbackCompleted: manual.completed,
		},
	}
}

type crawlerScrapeFoundNode struct {
	node    ktable.Node
	fixture crawlerScrapeNodePost
}

func crawlerScrapeFindNode(table ktable.Table, id protocol.ID) crawlerScrapeFoundNode {
	fixture := crawlerScrapeNodePost{ID: id.String()}
	for _, node := range table.GetClosestNodes(id) {
		if node.ID() != id {
			continue
		}
		addr := crawlerScrapeProjectAddress(node.Addr())
		fixture.Present = true
		fixture.Addr = &addr
		fixture.Responded = !node.Time().IsZero()
		return crawlerScrapeFoundNode{node: node, fixture: fixture}
	}
	return crawlerScrapeFoundNode{fixture: fixture}
}

func crawlerScrapeRequestValue(hash int, addr string) *nodeHasPeersForHash {
	return &nodeHasPeersForHash{infoHash: crawlerPingWorkerID(hash), node: netip.MustParseAddrPort(addr)}
}

func crawlerScrapeOutcomeValue(
	kind string,
	responseID int,
	values []string,
	nodes []crawlerScrapeNode,
	peersBloomHex string,
	seedersBloomHex string,
) *crawlerScrapeOutcome {
	projectedValues := make([]crawlerScrapeAddress, 0, len(values))
	for _, value := range values {
		projectedValues = append(projectedValues, crawlerScrapeProjectAddress(netip.MustParseAddrPort(value)))
	}
	if nodes == nil {
		nodes = []crawlerScrapeNode{}
	}
	return &crawlerScrapeOutcome{
		Kind: kind, ResponseID: crawlerPingWorkerID(responseID).String(), Values: projectedValues, Nodes: nodes,
		PeersBloomHex: peersBloomHex, SeedersBloomHex: seedersBloomHex,
	}
}

func crawlerScrapeNodeValue(id int, addr string) crawlerScrapeNode {
	return crawlerScrapeProjectNode(crawlerPingWorkerID(id), netip.MustParseAddrPort(addr))
}

func crawlerScrapeProjectRequest(request nodeHasPeersForHash) crawlerScrapeRequest {
	return crawlerScrapeRequest{InfoHash: request.infoHash.String(), Node: crawlerScrapeProjectAddress(request.node)}
}

func crawlerScrapeProjectNode(id protocol.ID, addr netip.AddrPort) crawlerScrapeNode {
	return crawlerScrapeNode{ID: id.String(), Addr: crawlerScrapeProjectAddress(addr)}
}

func crawlerScrapeProjectAddress(addr netip.AddrPort) crawlerScrapeAddress {
	scope, _ := strconv.ParseUint(addr.Addr().Zone(), 10, 32)
	return crawlerScrapeAddress{IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: uint32(scope)}
}

func crawlerScrapeProjectIP(addr netip.Addr) crawlerScrapeIP {
	scope, _ := strconv.ParseUint(addr.Zone(), 10, 32)
	return crawlerScrapeIP{IP: addr.WithZone("").String(), Scope: uint32(scope)}
}

func crawlerScrapeAddr(addr crawlerScrapeAddress) netip.AddrPort {
	ip := netip.MustParseAddr(addr.IP)
	if addr.Scope != 0 {
		ip = ip.WithZone(strconv.FormatUint(uint64(addr.Scope), 10))
	}
	return netip.AddrPortFrom(ip, addr.Port)
}

func crawlerScrapePatternHex(ip net.IP) string {
	var filter dhtwire.ScrapeBloomFilter
	filter.AddIP(ip)
	return hex.EncodeToString(filter[:])
}

func crawlerScrapeFilter(value string) bloom.Filter {
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != 256 {
		panic("scrape fixture bloom must be exact 256-byte hex")
	}
	var filter dhtwire.ScrapeBloomFilter
	copy(filter[:], raw)
	return bloom.FromScrape(filter)
}

func crawlerScrapeDescribeBloom(t *testing.T, filter *bloom.Filter) crawlerScrapeBloom {
	t.Helper()
	words := filter.BitSet().Words()
	raw := make([]byte, len(words)*8)
	for index, word := range words {
		binary.BigEndian.PutUint64(raw[index*8:], word)
	}
	if len(raw) != 256 {
		t.Fatalf("scrape handoff bloom has %d bytes, want 256", len(raw))
	}
	return crawlerScrapeBloom{BloomHex: hex.EncodeToString(raw)}
}

func crawlerScrapeProjectHandoff(t *testing.T, handoff infoHashWithScrape) crawlerScrapeHandoff {
	t.Helper()
	return crawlerScrapeHandoff{
		InfoHash: handoff.infoHash.String(), Node: crawlerScrapeProjectAddress(handoff.node),
		SeedersBloom: crawlerScrapeDescribeBloom(t, &handoff.bfsd),
		PeersBloom:   crawlerScrapeDescribeBloom(t, &handoff.bfpe),
	}
}

func crawlerScrapeWait(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type crawlerScrapeASTSpec struct {
	key  string
	path string
	kind string
	name string
}

func crawlerScrapeNormalizedASTDigests(t *testing.T) map[string]string {
	t.Helper()
	specifications := []crawlerScrapeASTSpec{
		{key: "batching.In", path: "internal/concurrency/batching_channel.go", kind: "func", name: "In"},
		{key: "batching.NewBatchingChannel", path: "internal/concurrency/batching_channel.go", kind: "func", name: "NewBatchingChannel"},
		{key: "batching.Out", path: "internal/concurrency/batching_channel.go", kind: "func", name: "Out"},
		{key: "buffered.In", path: "internal/concurrency/buffered_concurrent_channel.go", kind: "func", name: "In"},
		{key: "buffered.NewBufferedConcurrentChannel", path: "internal/concurrency/buffered_concurrent_channel.go", kind: "func", name: "NewBufferedConcurrentChannel"},
		{key: "buffered.Run", path: "internal/concurrency/buffered_concurrent_channel.go", kind: "func", name: "Run"},
		{key: "bloom.FromScrape", path: "internal/bloom/bloom.go", kind: "func", name: "FromScrape"},
		{key: "client.GetPeersScrapeResult", path: "internal/protocol/dht/client/interface.go", kind: "type", name: "GetPeersScrapeResult"},
		{key: "client.serverAdapter.GetPeersScrape", path: "internal/protocol/dht/client/server_adapter.go", kind: "func", name: "GetPeersScrape"},
		{key: "config.NewDefaultConfig", path: "internal/dhtcrawler/config.go", kind: "func", name: "NewDefaultConfig"},
		{key: "crawler.infoHashWithScrape", path: "internal/dhtcrawler/crawler.go", kind: "type", name: "infoHashWithScrape"},
		{key: "crawler.nodeHasPeersForHash", path: "internal/dhtcrawler/crawler.go", kind: "type", name: "nodeHasPeersForHash"},
		{key: "crawler.start", path: "internal/dhtcrawler/crawler.go", kind: "func", name: "start"},
		{key: "discovery.NewDiscoveredNodes", path: "internal/dhtcrawler/discovered_nodes.go", kind: "func", name: "NewDiscoveredNodes"},
		{key: "dht.ScrapeBloomFilter.ToBloomFilter", path: "internal/protocol/dht/scrape.go", kind: "func", name: "ToBloomFilter"},
		{key: "factory.New", path: "internal/dhtcrawler/factory.go", kind: "func", name: "New"},
		{key: "ktable.DropAddr", path: "internal/protocol/dht/ktable/command.go", kind: "type", name: "DropAddr"},
		{key: "ktable.NodeResponded", path: "internal/protocol/dht/ktable/node.go", kind: "func", name: "NodeResponded"},
		{key: "ktable.PutNode", path: "internal/protocol/dht/ktable/command.go", kind: "type", name: "PutNode"},
		{key: "scrape.requestScrape", path: "internal/dhtcrawler/scrape.go", kind: "func", name: "requestScrape"},
		{key: "scrape.runScrape", path: "internal/dhtcrawler/scrape.go", kind: "func", name: "runScrape"},
	}
	digests := make(map[string]string, len(specifications))
	missing := false
	for _, specification := range specifications {
		node, fileSet := crawlerScrapeFindASTNode(t, specification)
		var normalized bytes.Buffer
		if err := format.Node(&normalized, fileSet, node); err != nil {
			t.Fatal(err)
		}
		actual := fmt.Sprintf("%x", sha256.Sum256(normalized.Bytes()))
		digests[specification.key] = actual
		expected := crawlerScrapeExpectedNormalizedASTSHA256[specification.key]
		if expected == "" {
			missing = true
		} else if actual != expected {
			t.Fatalf("normalized AST SHA-256 %s = %s, want %s", specification.key, actual, expected)
		}
	}
	if missing {
		encoded, err := json.MarshalIndent(digests, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		t.Fatalf("fill crawlerScrapeExpectedNormalizedASTSHA256 with:\n%s", encoded)
	}
	return digests
}

func crawlerScrapeFindASTNode(t *testing.T, specification crawlerScrapeASTSpec) (ast.Node, *token.FileSet) {
	t.Helper()
	fileSet := token.NewFileSet()
	path := filepath.Join(crawlerScrapeRoot(t), specification.path)
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		switch typed := declaration.(type) {
		case *ast.FuncDecl:
			if specification.kind == "func" && typed.Name.Name == specification.name {
				return typed, fileSet
			}
		case *ast.GenDecl:
			if specification.kind != "type" {
				continue
			}
			for _, raw := range typed.Specs {
				typeSpec, ok := raw.(*ast.TypeSpec)
				if ok && typeSpec.Name.Name == specification.name {
					return typeSpec, fileSet
				}
			}
		}
	}
	t.Fatalf("%s %s not found in %s", specification.kind, specification.name, specification.path)
	return nil, nil
}

func crawlerScrapePrerequisiteDigests(t *testing.T) map[string]string {
	t.Helper()
	want := map[string]string{
		"testdata/parity/dht/peer_sample_client.jsonl":           "8c432a1555587a0c3dff51af3191c689adb3a2eda8b6515975ee1470b4bdfe51",
		"testdata/parity/dht/scrape_bloom.jsonl":                 "760f868a2cb53d8342e02c84b99ec0335fa20df52d5d2695b00d3f7e2d7ac287",
		"testdata/parity/dht/ktable_core.jsonl":                  "b49854c20df24afec5f9bf76c22b2bdd12ca0a629cd3f199a742d44adf99844e",
		"testdata/parity/dht/ktable_temporal.jsonl":              "03178e62efbc40519ccc0496204a081469ef49cf6b1a2336cff39b474a745444",
		"testdata/parity/dht/dht_crawler_info_hash_triage.jsonl": "52eda840f872225cc34f8cf12edc2e4621e8a1fef569abf34a50f4a3bd9896f8",
		"testdata/parity/dht/dht_crawler_discovered_nodes.jsonl": "ae6d867378a227284aa0cd93e9120d70afbec1c5e3b19a9f64e09edace4190e0",
		"testdata/parity/dht/dht_crawler_get_peers.jsonl":        "82b694fece9e46c05aefaab76bc05b78462bc04824bf6b83bb77eb544b7f0844",
	}
	crawlerScrapeValidateDigests(t, want)
	return want
}

func crawlerScrapeSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	want := map[string]string{
		"internal/bloom/bloom.go":                             "7fd2ef4970e108eb6b66d05f73aa0772864a93bdb49bee8e27697a321a8a9106",
		"internal/concurrency/batching_channel.go":            "72b3c9fd5fbc8ecbfb0ba2bc2ed5e6c1d45de01f03d3e015b2467f114ec70975",
		"internal/concurrency/buffered_concurrent_channel.go": "4be882800ec66d0c1709319fe029d61773c3f4a37bdb409e3a2f7d5d415d954c",
		"internal/dhtcrawler/config.go":                       "b3cac15378cdca0f21c5f21f37aeb0679815d5bacd16bfa0c3bac2af56db87ef",
		"internal/dhtcrawler/crawler.go":                      "ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6",
		"internal/dhtcrawler/discovered_nodes.go":             "22806cabf39173df71010a54d874a4319458f1715308834be828dbdb99767027",
		"internal/dhtcrawler/factory.go":                      "ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6",
		"internal/dhtcrawler/scrape.go":                       "8450576571bc044b1a85cb013ff6b330683b0b2b6e188110614120c3bafc320a",
		"internal/protocol/id.go":                             "e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f",
		"internal/protocol/dht/scrape.go":                     "7dd152311451eb95c580bb7e49822a51b775bd532bc2add14c9feea8432af6bd",
		"internal/protocol/dht/client/interface.go":           "477139d727ea685538bccfb0be114ab4fa43556cbdb70d5492a074f24482389f",
		"internal/protocol/dht/client/server_adapter.go":      "51334196660c0baeb730b1968f70db06af2622ea706de3e093fad39420539afa",
		"internal/protocol/dht/ktable/command.go":             "575e58a01856db0746281c3a66a95d6d5483452fb8ab20dc6379ffbc45cedf11",
		"internal/protocol/dht/ktable/keyspace.go":            "fe0894e7df90dcfc85b10c72bba3c55d639fff3030735d78172d0b9fdf761573",
		"internal/protocol/dht/ktable/node.go":                "93ed9a76a7cd0f50ee3ad255c6e77a8d19e5fe17081edc6238c5efab4983b3c3",
		"internal/protocol/dht/ktable/query.go":               "103ec27a7904bdbbbd91f3ea1dae1f4d6ea3b3d6652757a6ab8ddbf598a7060e",
		"internal/protocol/dht/ktable/reverse_map.go":         "31e65f7b3b108e13c11772d375f97d7973b00dfc4df490d676a458d4f9a05213",
		"internal/protocol/dht/ktable/table.go":               "68e3caf4394b2692fd9358224cce2b70ae3d90d920097bd28885b6b3bb77848f",
	}
	crawlerScrapeValidateDigests(t, want)
	return want
}

func crawlerScrapeModuleLines(t *testing.T) map[string][]string {
	t.Helper()
	want := map[string][]string{
		"go.mod": {"github.com/bits-and-blooms/bloom/v3 v3.7.0"},
		"go.sum": {
			"github.com/bits-and-blooms/bloom/v3 v3.7.0 h1:VfknkqV4xI+PsaDIsoHueyxVDZrfvMn56jeWUzvzdls=",
			"github.com/bits-and-blooms/bloom/v3 v3.7.0/go.mod h1:VKlUSvp0lFIYqxJjzdnSsZEw4iHb1kOL2tfHTgyJBHg=",
		},
	}
	for path, lines := range want {
		contents, err := os.ReadFile(filepath.Join(crawlerScrapeRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range lines {
			if !bytes.Contains(contents, []byte(line+"\n")) {
				t.Fatalf("%s missing exact module line %q", path, line)
			}
		}
	}
	return want
}

func crawlerScrapeValidateDigests(t *testing.T, want map[string]string) {
	t.Helper()
	for path, expected := range want {
		contents, err := os.ReadFile(filepath.Join(crawlerScrapeRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		actual := fmt.Sprintf("%x", sha256.Sum256(contents))
		if actual != expected {
			t.Fatalf("%s SHA-256 = %s, want %s", path, actual, expected)
		}
	}
}

func crawlerScrapeRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve scrape generator source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
}

func crawlerScrapeReconcileFixtures(t *testing.T, fixtures []crawlerScrapeFixture) {
	t.Helper()
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	for _, fixture := range fixtures {
		if err := encoder.Encode(fixture); err != nil {
			t.Fatal(err)
		}
	}
	actualHash := fmt.Sprintf("%x", sha256.Sum256(encoded.Bytes()))
	if crawlerScrapeFixtureSHA256 != "" && actualHash != crawlerScrapeFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerScrapeFixtureSHA256)
	}
	path := filepath.Join(crawlerScrapeRoot(t), "testdata/parity/dht/dht_crawler_scrape.jsonl")
	if *updateDHTCrawlerScrapeParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-scrape-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler scrape fixture is stale; rerun with -update-dht-crawler-scrape-parity")
	}
}

var _ concurrency.BufferedConcurrentChannel[nodeHasPeersForHash] = (*crawlerScrapeManualLane)(nil)
var _ concurrency.BatchingChannel[ktable.Node] = (*crawlerScrapeDiscovery)(nil)
var _ concurrency.BatchingChannel[infoHashWithScrape] = (*crawlerScrapeHandoffLane)(nil)
