package dhtcrawler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

var updateDHTCrawlerGetPeersParity = flag.Bool(
	"update-dht-crawler-get-peers-parity",
	false,
	"rewrite the Rust DHT crawler get-peers parity fixture",
)

const crawlerGetPeersFixtureSHA256 = "82b694fece9e46c05aefaab76bc05b78462bc04824bf6b83bb77eb544b7f0844"

var crawlerGetPeersFixtureIDs = [...]string{
	"production_source_factory_and_lifecycle_contract",
	"query_error_drops_request_ip_and_preserves_cause",
	"success_nodes_without_values_puts_responder_and_fans_out_before_no_peers",
	"success_preserves_node_and_peer_order_duplicates_and_puts_hash_before_metadata",
	"cancelled_before_client_return_still_puts_responder_and_hash_but_abandons_fanout_and_metadata",
	"cancel_after_one_discovery_retains_prefix_and_hash_but_abandons_suffix_and_metadata",
	"cancellation_at_blocked_metadata_send_keeps_table_prefix",
	"lane_error_is_swallowed",
}

var crawlerGetPeersExpectedNormalizedASTSHA256 = map[string]string{
	"buffered.In":                           "47b8d0cda8a3039f6d0ea101430404511705d63aafe3ea9edf95e7883f17bedb",
	"buffered.NewBufferedConcurrentChannel": "562428750b1aaf7a4811758daa63468461d995ac00f36e4d7b620fedfb4633ec",
	"buffered.Run":                          "0a8f90020ab24fb50cad498fcf376777cde3b5f6aa6424da3e66b15b54e3292f",
	"client.GetPeersResult":                 "8d03c6b4898e797df9d24b99d4a7bdda8d4c62d6a02e8396e800ccbf65f9ae20",
	"config.NewDefaultConfig":               "d044a4710817daf9a87dfab03ce22f138da3c6e1bf94d40bbbfd0fea70673f32",
	"crawler.infoHashWithPeers":             "9effbfa014d73da2f826c0c78a8388c8260ff76474f12d47cbab434303bf345e",
	"crawler.nodeHasPeersForHash":           "1e2206b038dd5c1b70dff5a29cdf044ad7133b4876db75723081ab37c3d3da58",
	"crawler.start":                         "d61a318ce626352ee4f5cd5dd48191d767bbfe45b6a9def673cd185eada4f67b",
	"factory.New":                           "0204a00fd63b275339d63d622865858571c153bc81fc738784a78e1c150fec80",
	"getpeers.requestPeersForHash":          "aa0925cbdac93e87b66eb828d9e3cd170633625243e6eab987569a9bb30fc880",
	"getpeers.runGetPeers":                  "82b1c5e9584e838a171a2d40acd1e5a0719d49a010d0e15076ea6d5e3d21a3b0",
	"ktable.DropAddr":                       "ab8ca0a52e22a72b0e37325cbccccf98de5211fc415e0ae139015ccdc9e91cd3",
	"ktable.HashPeer":                       "96c1c2acc982e6c5e6ded04aa22dcb83e7a49fe49032519fbad0a5c18f32f378",
	"ktable.NodeResponded":                  "52c5c68a8e6125a6d89839181e4dcb69bd62a1c857d2cf33c2f935d9c521e3d4",
	"ktable.PutHash":                        "75d31635e1942347ecffd0e0d7e3084c822a9526d6b34737ba954fc3c03250dd",
	"ktable.PutNode":                        "f85a3fc30b4e45d98dadc9b26ff08b34a49e97d01757e4aa8d69757b0cacdc00",
}

type crawlerGetPeersFixture struct {
	ID             string                  `json:"id"`
	Subsystem      string                  `json:"subsystem"`
	Classification string                  `json:"classification"`
	Oracle         crawlerGetPeersOracle   `json:"oracle"`
	Input          crawlerGetPeersInput    `json:"input"`
	Expected       crawlerGetPeersExpected `json:"expected"`
}

type crawlerGetPeersOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Lane        string `json:"lane"`
	Client      string `json:"client"`
	Table       string `json:"table"`
	Discovery   string `json:"discovery"`
	Metadata    string `json:"metadata"`
	Clock       string `json:"clock"`
}

type crawlerGetPeersInput struct {
	Kind                     string                      `json:"kind"`
	Requests                 []crawlerGetPeersRequest    `json:"requests"`
	Outcomes                 []crawlerGetPeersOutcome    `json:"outcomes"`
	TableSetup               []crawlerGetPeersTableSetup `json:"tableSetup"`
	DiscoveryMode            string                      `json:"discoveryMode,omitempty"`
	DiscoveryCapacity        int                         `json:"discoveryCapacity"`
	CancelBeforeClientReturn bool                        `json:"cancelBeforeClientReturn"`
	CancelAfterDiscoveries   int                         `json:"cancelAfterDiscoveries"`
	MetadataMode             string                      `json:"metadataMode,omitempty"`
	MetadataCapacity         int                         `json:"metadataCapacity"`
	CancelAtMetadataInCall   int                         `json:"cancelAtMetadataInCall"`
	LaneReturnError          bool                        `json:"laneReturnError"`
}

type crawlerGetPeersRequest struct {
	InfoHash string                 `json:"infoHash"`
	Node     crawlerGetPeersAddress `json:"node"`
}

type crawlerGetPeersOutcome struct {
	Kind       string                   `json:"kind"`
	ResponseID string                   `json:"responseId"`
	Values     []crawlerGetPeersAddress `json:"values"`
	Nodes      []crawlerGetPeersNode    `json:"nodes"`
}

type crawlerGetPeersNode struct {
	ID   string                 `json:"id"`
	Addr crawlerGetPeersAddress `json:"addr"`
}

type crawlerGetPeersAddress struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope"`
}

type crawlerGetPeersIP struct {
	IP    string `json:"ip"`
	Scope uint32 `json:"scope"`
}

type crawlerGetPeersTableSetup struct {
	Kind string                 `json:"kind"`
	ID   string                 `json:"id"`
	Addr crawlerGetPeersAddress `json:"addr"`
}

type crawlerGetPeersExpected struct {
	ClientCalls        []crawlerGetPeersClientCall `json:"clientCalls"`
	SameContext        bool                        `json:"sameContext"`
	BatchCalls         int                         `json:"batchCalls"`
	Commands           []crawlerGetPeersCommand    `json:"commands"`
	DiscoveryInCalls   int                         `json:"discoveryInCalls"`
	Discoveries        []crawlerGetPeersNode       `json:"discoveries"`
	MetadataInCalls    int                         `json:"metadataInCalls"`
	MetadataDeliveries []crawlerGetPeersMetadata   `json:"metadataDeliveries"`
	Events             []string                    `json:"events"`
	TablePost          crawlerGetPeersTablePost    `json:"tablePost"`
	RunReturned        bool                        `json:"runReturned"`
	ContextCancelled   bool                        `json:"contextCancelled"`
	CallbackCompleted  bool                        `json:"callbackCompleted"`
	Source             *crawlerGetPeersSource      `json:"source,omitempty"`
}

type crawlerGetPeersClientCall struct {
	Node     crawlerGetPeersAddress `json:"node"`
	InfoHash string                 `json:"infoHash"`
}

type crawlerGetPeersCommand struct {
	Kind                   string                   `json:"kind"`
	ID                     string                   `json:"id,omitempty"`
	Addr                   *crawlerGetPeersAddress  `json:"addr,omitempty"`
	DropIP                 *crawlerGetPeersIP       `json:"dropIp,omitempty"`
	Peers                  []crawlerGetPeersAddress `json:"peers"`
	OptionCount            int                      `json:"optionCount"`
	Reason                 string                   `json:"reason,omitempty"`
	ErrorIdentityPreserved bool                     `json:"errorIdentityPreserved"`
	StoredResponded        bool                     `json:"storedResponded"`
}

type crawlerGetPeersMetadata struct {
	InfoHash string                   `json:"infoHash"`
	Node     crawlerGetPeersAddress   `json:"node"`
	Peers    []crawlerGetPeersAddress `json:"peers"`
}

type crawlerGetPeersTablePost struct {
	Nodes []crawlerGetPeersNodePost `json:"nodes"`
	Hash  *crawlerGetPeersHashPost  `json:"hash,omitempty"`
}

type crawlerGetPeersNodePost struct {
	ID              string                  `json:"id"`
	Present         bool                    `json:"present"`
	Addr            *crawlerGetPeersAddress `json:"addr,omitempty"`
	Responded       bool                    `json:"responded"`
	RetainedDropped bool                    `json:"retainedDropped"`
}

type crawlerGetPeersHashPost struct {
	ID    string                   `json:"id"`
	Found bool                     `json:"found"`
	Peers []crawlerGetPeersAddress `json:"peers"`
}

type crawlerGetPeersSource struct {
	RunErrorIgnored                        bool              `json:"runErrorIgnored"`
	SharedCallbackContext                  bool              `json:"sharedCallbackContext"`
	ErrorDropsRequestIPAndScopeWithoutPort bool              `json:"errorDropsRequestIpAndScopeWithoutPort"`
	ErrorReasonWrapsCause                  bool              `json:"errorReasonWrapsCause"`
	SuccessUsesResponseID                  bool              `json:"successUsesResponseId"`
	SuccessUsesRequestAddress              bool              `json:"successUsesRequestAddress"`
	SuccessUsesNodeRespondedOption         bool              `json:"successUsesNodeRespondedOption"`
	NoPostClientCancellationBeforePutNode  bool              `json:"noPostClientCancellationBeforePutNode"`
	DiscoveryBeforePeerPresenceCheck       bool              `json:"discoveryBeforePeerPresenceCheck"`
	EmptyValuesError                       string            `json:"emptyValuesError"`
	DiscoveryTimeoutMS                     int               `json:"discoveryTimeoutMs"`
	DiscoveryUsesResponseOrder             bool              `json:"discoveryUsesResponseOrder"`
	DiscoveryCancelBreakLabelled           bool              `json:"discoveryCancelBreakLabelled"`
	DiscoveryCancelBreakScope              string            `json:"discoveryCancelBreakScope"`
	DiscoveryCancellationRetainsPrefix     bool              `json:"discoveryCancellationRetainsPrefix"`
	DiscoveryCancellationScansSuffix       bool              `json:"discoveryCancellationScansSuffix"`
	DiscoveryInAccessorEvaluatedForSuffix  bool              `json:"discoveryInAccessorEvaluatedForSuffix"`
	PeerOrderAndDuplicatesPreserved        bool              `json:"peerOrderAndDuplicatesPreserved"`
	PutHashPrecedesMetadata                bool              `json:"putHashPrecedesMetadata"`
	MetadataCancellationPreservesPutHash   bool              `json:"metadataCancellationPreservesPutHash"`
	ProductionGetPeersCapacity             int               `json:"productionGetPeersCapacity"`
	ProductionGetPeersConcurrency          int               `json:"productionGetPeersConcurrency"`
	ProductionMetadataCapacity             int               `json:"productionMetadataCapacity"`
	ProductionMetadataConcurrency          int               `json:"productionMetadataConcurrency"`
	DefaultScalingFactor                   int               `json:"defaultScalingFactor"`
	ConsumerDequeuesBeforeSemaphore        bool              `json:"consumerDequeuesBeforeSemaphore"`
	ConsumerCallbacksDetached              bool              `json:"consumerCallbacksDetached"`
	ConsumerCallbacksJoined                bool              `json:"consumerCallbacksJoined"`
	MaximumRetainedWork                    string            `json:"maximumRetainedWork"`
	ClosedInputChecksOpenBoolean           bool              `json:"closedInputChecksOpenBoolean"`
	ClosedInputOutcome                     string            `json:"closedInputOutcome"`
	ProductionDiscoveryCapacity            int               `json:"productionDiscoveryCapacity"`
	ProductionDiscoveryMaxBatchSize        int               `json:"productionDiscoveryMaxBatchSize"`
	ProductionDiscoveryIntervalMS          int               `json:"productionDiscoveryIntervalMs"`
	ProductionDiscoveryOutputCapacity      int               `json:"productionDiscoveryOutputCapacity"`
	StartLaunchesWorkerDetached            bool              `json:"startLaunchesWorkerDetached"`
	StartWaitsOnlyStopped                  bool              `json:"startWaitsOnlyStopped"`
	StartDefersSharedContextCancel         bool              `json:"startDefersSharedContextCancel"`
	StartJoinsWorkerOrCallbacks            bool              `json:"startJoinsWorkerOrCallbacks"`
	NormalizedASTSHA256                    map[string]string `json:"normalizedAstSha256"`
	PrerequisiteFixtureSHA256              map[string]string `json:"prerequisiteFixtureSha256"`
	EvidenceCommit                         map[string]string `json:"evidenceCommit"`
	SourceSHA256                           map[string]string `json:"sourceSha256"`
	Nonclaims                              []string          `json:"nonclaims"`
	Evidence                               string            `json:"evidence"`
}

type crawlerGetPeersEventLog struct {
	mutex  sync.Mutex
	events []string
}

func (l *crawlerGetPeersEventLog) append(event string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.events = append(l.events, event)
}

func (l *crawlerGetPeersEventLog) snapshot() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return append([]string{}, l.events...)
}

type crawlerGetPeersManualLane struct {
	requests  []nodeHasPeersForHash
	returnErr error
	events    *crawlerGetPeersEventLog
	completed bool
}

func (*crawlerGetPeersManualLane) In() chan<- nodeHasPeersForHash {
	panic("get-peers worker must not request its input sender")
}

func (l *crawlerGetPeersManualLane) Run(_ context.Context, callback func(nodeHasPeersForHash)) error {
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

type crawlerGetPeersClient struct {
	client.Client
	wantContext        context.Context
	outcomes           []crawlerGetPeersOutcome
	cancelBeforeReturn context.CancelFunc
	events             *crawlerGetPeersEventLog
	calls              []crawlerGetPeersClientCall
	sameContext        bool
}

var crawlerGetPeersSentinel = errors.New("oracle get_peers failure")

func (c *crawlerGetPeersClient) GetPeers(
	ctx context.Context,
	addr netip.AddrPort,
	infoHash protocol.ID,
) (client.GetPeersResult, error) {
	c.sameContext = c.sameContext && ctx == c.wantContext
	c.calls = append(c.calls, crawlerGetPeersClientCall{
		Node: projectCrawlerGetPeersAddress(addr), InfoHash: infoHash.String(),
	})
	c.events.append("client_get_peers:" + strconv.Itoa(len(c.calls)))
	outcome := c.outcomes[len(c.calls)-1]
	if c.cancelBeforeReturn != nil {
		c.cancelBeforeReturn()
		c.cancelBeforeReturn = nil
	}
	result := client.GetPeersResult{ID: protocol.MustParseID(outcome.ResponseID)}
	for _, value := range outcome.Values {
		result.Values = append(result.Values, crawlerGetPeersAddr(value))
	}
	for _, node := range outcome.Nodes {
		result.Nodes = append(result.Nodes, client.NodeInfo{
			ID: protocol.MustParseID(node.ID), Addr: crawlerGetPeersAddr(node.Addr),
		})
	}
	if outcome.Kind == "error" {
		return result, crawlerGetPeersSentinel
	}
	return result, nil
}

type crawlerGetPeersDiscovery struct {
	input      chan ktable.Node
	events     *crawlerGetPeersEventLog
	mutex      sync.Mutex
	inCalls    int
	deliveries []ktable.Node
}

func (d *crawlerGetPeersDiscovery) In() chan<- ktable.Node {
	d.mutex.Lock()
	d.inCalls++
	call := d.inCalls
	d.mutex.Unlock()
	d.events.append("discovery_in:" + strconv.Itoa(call))
	return d.input
}

func (*crawlerGetPeersDiscovery) Out() <-chan []ktable.Node {
	panic("get-peers worker must not request discovered-node output")
}

func (d *crawlerGetPeersDiscovery) collect(node ktable.Node) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.deliveries = append(d.deliveries, node)
}

func (d *crawlerGetPeersDiscovery) drainBuffered() {
	for len(d.input) > 0 {
		d.collect(<-d.input)
	}
}

func (d *crawlerGetPeersDiscovery) snapshot() (int, []crawlerGetPeersNode) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	nodes := make([]crawlerGetPeersNode, 0, len(d.deliveries))
	for _, node := range d.deliveries {
		nodes = append(nodes, projectCrawlerGetPeersNode(node.ID(), node.Addr()))
	}
	return d.inCalls, nodes
}

type crawlerGetPeersMetadataLane struct {
	input          chan infoHashWithPeers
	events         *crawlerGetPeersEventLog
	cancel         context.CancelFunc
	cancelAtInCall int
	mutex          sync.Mutex
	inCalls        int
}

func (l *crawlerGetPeersMetadataLane) In() chan<- infoHashWithPeers {
	l.mutex.Lock()
	l.inCalls++
	call := l.inCalls
	l.mutex.Unlock()
	l.events.append("metadata_in:" + strconv.Itoa(call))
	if l.cancel != nil && call == l.cancelAtInCall {
		l.cancel()
	}
	return l.input
}

func (*crawlerGetPeersMetadataLane) Run(context.Context, func(infoHashWithPeers)) error {
	panic("get-peers worker must not run the metadata lane")
}

func (l *crawlerGetPeersMetadataLane) snapshot() (int, []crawlerGetPeersMetadata) {
	l.mutex.Lock()
	calls := l.inCalls
	l.mutex.Unlock()
	deliveries := make([]crawlerGetPeersMetadata, 0, len(l.input))
	for len(l.input) > 0 {
		deliveries = append(deliveries, projectCrawlerGetPeersMetadata(<-l.input))
	}
	return calls, deliveries
}

type crawlerGetPeersTracingTable struct {
	ktable.Table
	sentinel   error
	events     *crawlerGetPeersEventLog
	batchCalls int
	commands   []crawlerGetPeersCommand
}

func (t *crawlerGetPeersTracingTable) BatchCommand(commands ...ktable.Command) {
	t.batchCalls++
	start := len(t.commands)
	for _, raw := range commands {
		switch command := raw.(type) {
		case ktable.DropAddr:
			ip := projectCrawlerGetPeersIP(command.Addr)
			t.commands = append(t.commands, crawlerGetPeersCommand{
				Kind: "drop_addr", DropIP: &ip, Peers: []crawlerGetPeersAddress{},
				Reason: command.Reason.Error(), ErrorIdentityPreserved: errors.Is(command.Reason, t.sentinel),
			})
			t.events.append("batch_drop_addr")
		case ktable.PutNode:
			addr := projectCrawlerGetPeersAddress(command.Addr)
			t.commands = append(t.commands, crawlerGetPeersCommand{
				Kind: "put_node", ID: command.ID.String(), Addr: &addr,
				Peers: []crawlerGetPeersAddress{}, OptionCount: len(command.Options),
			})
			t.events.append("batch_put_node")
		case ktable.PutHash:
			peers := make([]crawlerGetPeersAddress, 0, len(command.Peers))
			for _, peer := range command.Peers {
				peers = append(peers, projectCrawlerGetPeersAddress(peer.Addr))
			}
			t.commands = append(t.commands, crawlerGetPeersCommand{
				Kind: "put_hash", ID: command.ID.String(), Peers: peers,
				OptionCount: len(command.Options),
			})
			t.events.append("batch_put_hash")
		default:
			panic(fmt.Sprintf("unexpected get-peers command %T", raw))
		}
	}
	t.Table.BatchCommand(commands...)
	for index := start; index < len(t.commands); index++ {
		command := &t.commands[index]
		if command.Kind == "put_node" {
			post := crawlerGetPeersFindNode(t.Table, protocol.MustParseID(command.ID))
			command.StoredResponded = post.fixture.Present && post.fixture.Responded
		}
	}
}

type crawlerGetPeersScenario struct {
	id                       string
	classification           string
	request                  *nodeHasPeersForHash
	outcome                  *crawlerGetPeersOutcome
	seedErrorNode            bool
	discoveryMode            string
	discoveryCapacity        int
	cancelBeforeClientReturn bool
	cancelAfterDiscoveries   int
	metadataMode             string
	metadataCapacity         int
	cancelAtMetadataInCall   int
	laneReturnError          bool
}

func TestGenerateDHTCrawlerGetPeersParity(t *testing.T) {
	fixtures := []crawlerGetPeersFixture{crawlerGetPeersSourceFixture(t)}
	fixtures = append(fixtures,
		runCrawlerGetPeersScenario(t, crawlerGetPeersScenario{
			id: "query_error_drops_request_ip_and_preserves_cause", classification: "RUNTIME_EXACT",
			request: crawlerGetPeersRequestValue(102, "[fe80::102%7]:7102"),
			outcome: crawlerGetPeersOutcomeValue("error", 202, nil, nil), seedErrorNode: true,
			discoveryMode: "unbuffered_no_receiver", metadataMode: "unbuffered_no_receiver",
		}),
		runCrawlerGetPeersScenario(t, crawlerGetPeersScenario{
			id: "success_nodes_without_values_puts_responder_and_fans_out_before_no_peers", classification: "RUNTIME_EXACT",
			request: crawlerGetPeersRequestValue(103, "198.51.100.103:7103"),
			outcome: crawlerGetPeersOutcomeValue("success", 203, nil, []crawlerGetPeersNode{
				crawlerGetPeersNodeValue(13, "203.0.113.13:7213"),
				crawlerGetPeersNodeValue(14, "[2001:db8::14]:7214"),
			}),
			discoveryMode: "buffered_accept_all", discoveryCapacity: 2,
			metadataMode: "unbuffered_no_receiver",
		}),
		runCrawlerGetPeersScenario(t, crawlerGetPeersScenario{
			id: "success_preserves_node_and_peer_order_duplicates_and_puts_hash_before_metadata", classification: "RUNTIME_EXACT",
			request: crawlerGetPeersRequestValue(104, "198.51.100.104:7104"),
			outcome: crawlerGetPeersOutcomeValue("success", 204, []string{
				"203.0.113.40:7340", "203.0.113.40:7340", "203.0.113.40:7341", "[2001:db8::41]:7342",
			}, []crawlerGetPeersNode{
				crawlerGetPeersNodeValue(15, "203.0.113.15:7215"),
				crawlerGetPeersNodeValue(16, "203.0.113.16:7216"),
			}),
			discoveryMode: "buffered_accept_all", discoveryCapacity: 2,
			metadataMode: "buffered_accept_all", metadataCapacity: 1,
		}),
		runCrawlerGetPeersScenario(t, crawlerGetPeersScenario{
			id:             "cancelled_before_client_return_still_puts_responder_and_hash_but_abandons_fanout_and_metadata",
			classification: "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
			request:        crawlerGetPeersRequestValue(105, "198.51.100.105:7105"),
			outcome: crawlerGetPeersOutcomeValue("success", 205, []string{"203.0.113.50:7350"}, []crawlerGetPeersNode{
				crawlerGetPeersNodeValue(17, "203.0.113.17:7217"),
				crawlerGetPeersNodeValue(18, "203.0.113.18:7218"),
			}),
			cancelBeforeClientReturn: true, discoveryMode: "unbuffered_no_receiver",
			metadataMode: "unbuffered_no_receiver",
		}),
		runCrawlerGetPeersScenario(t, crawlerGetPeersScenario{
			id:             "cancel_after_one_discovery_retains_prefix_and_hash_but_abandons_suffix_and_metadata",
			classification: "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
			request:        crawlerGetPeersRequestValue(106, "198.51.100.106:7106"),
			outcome: crawlerGetPeersOutcomeValue("success", 206, []string{"203.0.113.60:7360"}, []crawlerGetPeersNode{
				crawlerGetPeersNodeValue(19, "203.0.113.19:7219"),
				crawlerGetPeersNodeValue(20, "203.0.113.20:7220"),
				crawlerGetPeersNodeValue(21, "203.0.113.21:7221"),
			}),
			discoveryMode: "unbuffered_cancel_after_prefix", cancelAfterDiscoveries: 1,
			metadataMode: "unbuffered_no_receiver",
		}),
		runCrawlerGetPeersScenario(t, crawlerGetPeersScenario{
			id:             "cancellation_at_blocked_metadata_send_keeps_table_prefix",
			classification: "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
			request:        crawlerGetPeersRequestValue(107, "198.51.100.107:7107"),
			outcome:        crawlerGetPeersOutcomeValue("success", 207, []string{"203.0.113.70:7370"}, nil),
			discoveryMode:  "unbuffered_no_receiver", metadataMode: "unbuffered_cancel_at_in",
			cancelAtMetadataInCall: 1,
		}),
		runCrawlerGetPeersScenario(t, crawlerGetPeersScenario{
			id: "lane_error_is_swallowed", classification: "GO_ONLY_LANE",
			discoveryMode: "unbuffered_no_receiver", metadataMode: "unbuffered_no_receiver",
			laneReturnError: true,
		}),
	)

	wantClassifications := [...]string{
		"SOURCE_ONLY", "RUNTIME_EXACT", "RUNTIME_EXACT", "RUNTIME_EXACT",
		"RUNTIME_WITH_OWNED_SHUTDOWN_DELTA", "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA",
		"RUNTIME_WITH_OWNED_SHUTDOWN_DELTA", "GO_ONLY_LANE",
	}
	if len(fixtures) != len(crawlerGetPeersFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerGetPeersFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerGetPeersFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerGetPeersFixtureIDs[index])
		}
		if fixture.Subsystem != "dht_crawler_get_peers" {
			t.Fatalf("fixture %s subsystem = %q", fixture.ID, fixture.Subsystem)
		}
		if fixture.Classification != wantClassifications[index] {
			t.Fatalf("fixture %s classification = %q, want %q", fixture.ID, fixture.Classification, wantClassifications[index])
		}
	}
	reconcileCrawlerGetPeersFixtures(t, fixtures)
}

func crawlerGetPeersSourceFixture(t *testing.T) crawlerGetPeersFixture {
	t.Helper()
	config := NewDefaultConfig()
	if config.ScalingFactor != 10 {
		t.Fatalf("default scaling factor = %d, want 10", config.ScalingFactor)
	}
	return crawlerGetPeersFixture{
		ID: crawlerGetPeersFixtureIDs[0], Subsystem: "dht_crawler_get_peers", Classification: "SOURCE_ONLY",
		Oracle: crawlerGetPeersOracle{
			Composition: "production_source_factory_and_lifecycle_freshness_gate",
			Determinism: "exact_normalized_AST_source_and_prerequisite_fixture_SHA256",
			Lane:        "production_BufferedConcurrentChannel_source_shape", Client: "production_Client_GetPeers_interface",
			Table: "production_KTable_command_and_query_source_shapes", Discovery: "production_BatchingChannel_source_shape",
			Metadata: "production_BufferedConcurrentChannel_source_shape", Clock: "timeout_and_NodeResponded_source_only",
		},
		Input: crawlerGetPeersInput{
			Kind: "source_contract", Requests: []crawlerGetPeersRequest{}, Outcomes: []crawlerGetPeersOutcome{},
			TableSetup: []crawlerGetPeersTableSetup{},
		},
		Expected: crawlerGetPeersExpected{
			ClientCalls: []crawlerGetPeersClientCall{}, Commands: []crawlerGetPeersCommand{},
			Discoveries: []crawlerGetPeersNode{}, MetadataDeliveries: []crawlerGetPeersMetadata{},
			Events: []string{}, TablePost: crawlerGetPeersTablePost{Nodes: []crawlerGetPeersNodePost{}}, RunReturned: false,
			Source: &crawlerGetPeersSource{
				RunErrorIgnored: true, SharedCallbackContext: true,
				ErrorDropsRequestIPAndScopeWithoutPort: true, ErrorReasonWrapsCause: true,
				SuccessUsesResponseID: true, SuccessUsesRequestAddress: true, SuccessUsesNodeRespondedOption: true,
				NoPostClientCancellationBeforePutNode: true, DiscoveryBeforePeerPresenceCheck: true,
				EmptyValuesError:   "no peers found",
				DiscoveryTimeoutMS: 1000, DiscoveryUsesResponseOrder: true,
				DiscoveryCancelBreakLabelled: false, DiscoveryCancelBreakScope: "select_only_not_for_loop",
				DiscoveryCancellationRetainsPrefix: true, DiscoveryCancellationScansSuffix: true,
				DiscoveryInAccessorEvaluatedForSuffix: true, PeerOrderAndDuplicatesPreserved: true,
				PutHashPrecedesMetadata: true, MetadataCancellationPreservesPutHash: true,
				ProductionGetPeersCapacity:    10 * int(config.ScalingFactor),
				ProductionGetPeersConcurrency: 20 * int(config.ScalingFactor),
				ProductionMetadataCapacity:    10 * int(config.ScalingFactor),
				ProductionMetadataConcurrency: 40 * int(config.ScalingFactor),
				DefaultScalingFactor:          int(config.ScalingFactor), ConsumerDequeuesBeforeSemaphore: true,
				ConsumerCallbacksDetached: true, ConsumerCallbacksJoined: false,
				MaximumRetainedWork:             "capacity_plus_concurrency_plus_one_acquire_waiter",
				ClosedInputChecksOpenBoolean:    false,
				ClosedInputOutcome:              "repeated_zero_value_callbacks_can_issue_invalid_zero_request_work",
				ProductionDiscoveryCapacity:     100 * int(config.ScalingFactor),
				ProductionDiscoveryMaxBatchSize: 10, ProductionDiscoveryIntervalMS: 10,
				ProductionDiscoveryOutputCapacity: 1, StartLaunchesWorkerDetached: true,
				StartWaitsOnlyStopped: true, StartDefersSharedContextCancel: true, StartJoinsWorkerOrCallbacks: false,
				NormalizedASTSHA256:       crawlerGetPeersNormalizedASTDigests(t),
				PrerequisiteFixtureSHA256: crawlerGetPeersPrerequisiteDigests(t),
				EvidenceCommit: map[string]string{
					"peer_client_oracle":      "1f00b40705ba527721208023ddec64220fb40729",
					"ktable_core_oracle":      "b345998fe0e3f3f99d35745588cbd8c375124ac8",
					"ktable_temporal_oracle":  "1df4d7a09f74e13e75ea2e1ab1dcfc67a130ed9d",
					"info_hash_triage_oracle": "6aece7ac7605507aaf5ccdcc9adf2497170b071d",
					"discovered_nodes_oracle": "069b3febcf1e270ffdaef9941bf56d494697bf2c",
					"typed_get_peers_route":   "a5e2276ea9e2d93a75c3af8f4226bf2c333d27be",
				},
				SourceSHA256: crawlerGetPeersSourceDigests(t),
				Nonclaims: []string{
					"exact_ready_select_tie_winner",
					"goroutine_callback_scheduling_completion_or_order",
					"semaphore_fairness",
					"closed_buffered_input_runtime_execution",
					"callback_join_guarantee",
					"actual_one_second_timeout_elapsed_in_runtime_rows",
					"arbitrary_side_effects_of_eagerly_evaluated_channel_accessors_beyond_recorded_In_call_counts",
					"send_to_closed_Go_channel_behavior",
					"exact_wall_clock_NodeResponded_timestamp",
					"KTable_map_iteration_eviction_or_internal_layout",
					"opaque_NodeOption_function_identity",
					"live_DNS_UDP_DHT_network_or_client_wire_behavior",
					"downstream_discovered_node_deduplication_scheduling_or_routing",
					"metainfo_requester_banning_or_blocking_behavior",
					"torrent_source_persistence_or_database_behavior",
					"production_throughput_application_supervisor_or_deployment_wiring",
					"arbitrary_textual_IPv6_zones_runtime_rows_use_unscoped_or_numeric_scope_only",
					"Rust_public_API_or_owned_task_lifecycle_no_Rust_consumer_exists_in_this_slice",
				},
				Evidence: "runtime rows execute actual runGetPeers and requestPeersForHash synchronously through controlled interfaces and an actual KTable; source-only facts freeze full declarations and files",
			},
		},
	}
}

func runCrawlerGetPeersScenario(t *testing.T, scenario crawlerGetPeersScenario) crawlerGetPeersFixture {
	t.Helper()
	events := &crawlerGetPeersEventLog{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manual := &crawlerGetPeersManualLane{events: events}
	input := crawlerGetPeersInput{
		Kind: "run_get_peers", Requests: []crawlerGetPeersRequest{}, Outcomes: []crawlerGetPeersOutcome{},
		TableSetup: []crawlerGetPeersTableSetup{}, DiscoveryMode: scenario.discoveryMode,
		DiscoveryCapacity: scenario.discoveryCapacity, CancelBeforeClientReturn: scenario.cancelBeforeClientReturn,
		CancelAfterDiscoveries: scenario.cancelAfterDiscoveries, MetadataMode: scenario.metadataMode,
		MetadataCapacity: scenario.metadataCapacity, CancelAtMetadataInCall: scenario.cancelAtMetadataInCall,
		LaneReturnError: scenario.laneReturnError,
	}
	if scenario.request != nil {
		manual.requests = []nodeHasPeersForHash{*scenario.request}
		input.Requests = []crawlerGetPeersRequest{projectCrawlerGetPeersRequest(*scenario.request)}
	}
	if scenario.outcome != nil {
		input.Outcomes = []crawlerGetPeersOutcome{*scenario.outcome}
	}
	if scenario.laneReturnError {
		manual.returnErr = errors.New("oracle lane failure")
	}

	base := ktable.New(ktable.Params{NodeID: protocol.ID{}}).Table
	var retainedSeed ktable.Node
	var seedID protocol.ID
	if scenario.seedErrorNode {
		seedID = crawlerPingWorkerID(2)
		base.PutNode(seedID, scenario.request.node)
		retainedSeed = crawlerGetPeersFindNode(base, seedID).node
		base.PutNode(seedID, scenario.request.node)
		input.TableSetup = []crawlerGetPeersTableSetup{{
			Kind: "put_same_node_twice_to_populate_reverse_map", ID: seedID.String(),
			Addr: projectCrawlerGetPeersAddress(scenario.request.node),
		}}
	}
	tracing := &crawlerGetPeersTracingTable{
		Table: base, sentinel: crawlerGetPeersSentinel, events: events,
	}
	scripted := &crawlerGetPeersClient{
		wantContext: ctx, sameContext: true, events: events,
	}
	if scenario.outcome != nil {
		scripted.outcomes = []crawlerGetPeersOutcome{*scenario.outcome}
	}
	if scenario.cancelBeforeClientReturn {
		scripted.cancelBeforeReturn = cancel
	}
	discovery := &crawlerGetPeersDiscovery{
		input: make(chan ktable.Node, scenario.discoveryCapacity), events: events,
	}
	var discoveryReceiverDone chan struct{}
	if scenario.cancelAfterDiscoveries > 0 {
		if scenario.cancelAfterDiscoveries != 1 {
			t.Fatal("controlled get-peers discovery prefix only supports one delivery")
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
	metadata := &crawlerGetPeersMetadataLane{
		input: make(chan infoHashWithPeers, scenario.metadataCapacity), events: events,
		cancel: cancel, cancelAtInCall: scenario.cancelAtMetadataInCall,
	}
	c := crawler{
		getPeers: manual, client: scripted, kTable: tracing,
		discoveredNodes: discovery, requestMetaInfo: metadata,
	}
	c.runGetPeers(ctx)
	if discoveryReceiverDone != nil {
		crawlerGetPeersWait(t, discoveryReceiverDone, "discovery prefix receiver")
	}
	discovery.drainBuffered()
	discoveryInCalls, discoveries := discovery.snapshot()
	metadataInCalls, metadataDeliveries := metadata.snapshot()

	tablePost := crawlerGetPeersTablePost{Nodes: []crawlerGetPeersNodePost{}}
	if scenario.seedErrorNode {
		post := crawlerGetPeersFindNode(base, seedID)
		post.fixture.RetainedDropped = retainedSeed != nil && retainedSeed.Dropped()
		tablePost.Nodes = append(tablePost.Nodes, post.fixture)
	}
	if scenario.outcome != nil && scenario.outcome.Kind == "success" {
		responseID := protocol.MustParseID(scenario.outcome.ResponseID)
		tablePost.Nodes = append(tablePost.Nodes, crawlerGetPeersFindNode(base, responseID).fixture)
	}
	if scenario.request != nil {
		tablePost.Hash = crawlerGetPeersFindHash(base, scenario.request.infoHash)
	}
	return crawlerGetPeersFixture{
		ID: scenario.id, Subsystem: "dht_crawler_get_peers", Classification: scenario.classification,
		Oracle: crawlerGetPeersOracle{
			Composition: "actual_runGetPeers_requestPeersForHash_manual_callback_lane_scripted_client_actual_KTable",
			Determinism: "synchronous_callback_controlled_channel_acceptance_and_explicit_cancellation_gates",
			Lane:        "manual_in_order_callback_interface", Client: "scripted_Client_GetPeers_override",
			Table: "tracing_wrapper_over_actual_KTable", Discovery: scenario.discoveryMode,
			Metadata: scenario.metadataMode, Clock: "NodeResponded_boolean_only_no_timestamp_assertion",
		},
		Input: input,
		Expected: crawlerGetPeersExpected{
			ClientCalls: append([]crawlerGetPeersClientCall{}, scripted.calls...), SameContext: scripted.sameContext,
			BatchCalls: tracing.batchCalls, Commands: append([]crawlerGetPeersCommand{}, tracing.commands...),
			DiscoveryInCalls: discoveryInCalls, Discoveries: discoveries,
			MetadataInCalls: metadataInCalls, MetadataDeliveries: metadataDeliveries,
			Events: events.snapshot(), TablePost: tablePost, RunReturned: true,
			ContextCancelled: ctx.Err() != nil, CallbackCompleted: manual.completed,
		},
	}
}

type crawlerGetPeersFoundNode struct {
	node    ktable.Node
	fixture crawlerGetPeersNodePost
}

func crawlerGetPeersFindNode(table ktable.Table, id protocol.ID) crawlerGetPeersFoundNode {
	fixture := crawlerGetPeersNodePost{ID: id.String()}
	for _, node := range table.GetClosestNodes(id) {
		if node.ID() != id {
			continue
		}
		addr := projectCrawlerGetPeersAddress(node.Addr())
		fixture.Present = true
		fixture.Addr = &addr
		fixture.Responded = !node.Time().IsZero()
		return crawlerGetPeersFoundNode{node: node, fixture: fixture}
	}
	return crawlerGetPeersFoundNode{fixture: fixture}
}

func crawlerGetPeersFindHash(table ktable.Table, id protocol.ID) *crawlerGetPeersHashPost {
	post := &crawlerGetPeersHashPost{ID: id.String(), Peers: []crawlerGetPeersAddress{}}
	result := table.GetHashOrClosestNodes(id)
	if !result.Found || result.Hash.ID() != id {
		return post
	}
	post.Found = true
	for _, peer := range result.Hash.Peers() {
		post.Peers = append(post.Peers, projectCrawlerGetPeersAddress(peer.Addr))
	}
	sort.Slice(post.Peers, func(i, j int) bool {
		left, right := post.Peers[i], post.Peers[j]
		if left.IP != right.IP {
			return left.IP < right.IP
		}
		if left.Scope != right.Scope {
			return left.Scope < right.Scope
		}
		return left.Port < right.Port
	})
	return post
}

func crawlerGetPeersRequestValue(hash int, addr string) *nodeHasPeersForHash {
	return &nodeHasPeersForHash{infoHash: crawlerPingWorkerID(hash), node: netip.MustParseAddrPort(addr)}
}

func crawlerGetPeersOutcomeValue(
	kind string,
	responseID int,
	values []string,
	nodes []crawlerGetPeersNode,
) *crawlerGetPeersOutcome {
	projectedValues := make([]crawlerGetPeersAddress, 0, len(values))
	for _, value := range values {
		projectedValues = append(projectedValues, projectCrawlerGetPeersAddress(netip.MustParseAddrPort(value)))
	}
	if nodes == nil {
		nodes = []crawlerGetPeersNode{}
	}
	return &crawlerGetPeersOutcome{
		Kind: kind, ResponseID: crawlerPingWorkerID(responseID).String(), Values: projectedValues, Nodes: nodes,
	}
}

func crawlerGetPeersNodeValue(id int, addr string) crawlerGetPeersNode {
	return projectCrawlerGetPeersNode(crawlerPingWorkerID(id), netip.MustParseAddrPort(addr))
}

func projectCrawlerGetPeersRequest(request nodeHasPeersForHash) crawlerGetPeersRequest {
	return crawlerGetPeersRequest{
		InfoHash: request.infoHash.String(), Node: projectCrawlerGetPeersAddress(request.node),
	}
}

func projectCrawlerGetPeersNode(id protocol.ID, addr netip.AddrPort) crawlerGetPeersNode {
	return crawlerGetPeersNode{ID: id.String(), Addr: projectCrawlerGetPeersAddress(addr)}
}

func projectCrawlerGetPeersMetadata(metadata infoHashWithPeers) crawlerGetPeersMetadata {
	peers := make([]crawlerGetPeersAddress, 0, len(metadata.peers))
	for _, peer := range metadata.peers {
		peers = append(peers, projectCrawlerGetPeersAddress(peer))
	}
	return crawlerGetPeersMetadata{
		InfoHash: metadata.infoHash.String(), Node: projectCrawlerGetPeersAddress(metadata.node), Peers: peers,
	}
}

func projectCrawlerGetPeersAddress(addr netip.AddrPort) crawlerGetPeersAddress {
	scope, _ := strconv.ParseUint(addr.Addr().Zone(), 10, 32)
	return crawlerGetPeersAddress{
		IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: uint32(scope),
	}
}

func projectCrawlerGetPeersIP(addr netip.Addr) crawlerGetPeersIP {
	scope, _ := strconv.ParseUint(addr.Zone(), 10, 32)
	return crawlerGetPeersIP{IP: addr.WithZone("").String(), Scope: uint32(scope)}
}

func crawlerGetPeersAddr(addr crawlerGetPeersAddress) netip.AddrPort {
	ip := netip.MustParseAddr(addr.IP)
	if addr.Scope != 0 {
		ip = ip.WithZone(strconv.FormatUint(uint64(addr.Scope), 10))
	}
	return netip.AddrPortFrom(ip, addr.Port)
}

func crawlerGetPeersWait(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type crawlerGetPeersASTSpec struct {
	key  string
	path string
	kind string
	name string
}

func crawlerGetPeersNormalizedASTDigests(t *testing.T) map[string]string {
	t.Helper()
	specifications := []crawlerGetPeersASTSpec{
		{key: "buffered.In", path: "internal/concurrency/buffered_concurrent_channel.go", kind: "func", name: "In"},
		{key: "buffered.NewBufferedConcurrentChannel", path: "internal/concurrency/buffered_concurrent_channel.go", kind: "func", name: "NewBufferedConcurrentChannel"},
		{key: "buffered.Run", path: "internal/concurrency/buffered_concurrent_channel.go", kind: "func", name: "Run"},
		{key: "client.GetPeersResult", path: "internal/protocol/dht/client/interface.go", kind: "type", name: "GetPeersResult"},
		{key: "config.NewDefaultConfig", path: "internal/dhtcrawler/config.go", kind: "func", name: "NewDefaultConfig"},
		{key: "crawler.infoHashWithPeers", path: "internal/dhtcrawler/crawler.go", kind: "type", name: "infoHashWithPeers"},
		{key: "crawler.nodeHasPeersForHash", path: "internal/dhtcrawler/crawler.go", kind: "type", name: "nodeHasPeersForHash"},
		{key: "crawler.start", path: "internal/dhtcrawler/crawler.go", kind: "func", name: "start"},
		{key: "factory.New", path: "internal/dhtcrawler/factory.go", kind: "func", name: "New"},
		{key: "getpeers.requestPeersForHash", path: "internal/dhtcrawler/get_peers.go", kind: "func", name: "requestPeersForHash"},
		{key: "getpeers.runGetPeers", path: "internal/dhtcrawler/get_peers.go", kind: "func", name: "runGetPeers"},
		{key: "ktable.DropAddr", path: "internal/protocol/dht/ktable/command.go", kind: "type", name: "DropAddr"},
		{key: "ktable.HashPeer", path: "internal/protocol/dht/ktable/hash.go", kind: "type", name: "HashPeer"},
		{key: "ktable.NodeResponded", path: "internal/protocol/dht/ktable/node.go", kind: "func", name: "NodeResponded"},
		{key: "ktable.PutHash", path: "internal/protocol/dht/ktable/command.go", kind: "type", name: "PutHash"},
		{key: "ktable.PutNode", path: "internal/protocol/dht/ktable/command.go", kind: "type", name: "PutNode"},
	}
	digests := make(map[string]string, len(specifications))
	missing := false
	for _, specification := range specifications {
		node, fileSet := crawlerGetPeersFindASTNode(t, specification)
		var normalized bytes.Buffer
		if err := format.Node(&normalized, fileSet, node); err != nil {
			t.Fatal(err)
		}
		actual := fmt.Sprintf("%x", sha256.Sum256(normalized.Bytes()))
		digests[specification.key] = actual
		expected := crawlerGetPeersExpectedNormalizedASTSHA256[specification.key]
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
		t.Fatalf("fill crawlerGetPeersExpectedNormalizedASTSHA256 with:\n%s", encoded)
	}
	return digests
}

func crawlerGetPeersFindASTNode(t *testing.T, specification crawlerGetPeersASTSpec) (ast.Node, *token.FileSet) {
	t.Helper()
	fileSet := token.NewFileSet()
	path := filepath.Join(crawlerGetPeersRoot(t), specification.path)
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

func crawlerGetPeersPrerequisiteDigests(t *testing.T) map[string]string {
	t.Helper()
	want := map[string]string{
		"testdata/parity/dht/peer_sample_client.jsonl":           "8c432a1555587a0c3dff51af3191c689adb3a2eda8b6515975ee1470b4bdfe51",
		"testdata/parity/dht/ktable_core.jsonl":                  "b49854c20df24afec5f9bf76c22b2bdd12ca0a629cd3f199a742d44adf99844e",
		"testdata/parity/dht/ktable_temporal.jsonl":              "03178e62efbc40519ccc0496204a081469ef49cf6b1a2336cff39b474a745444",
		"testdata/parity/dht/dht_crawler_info_hash_triage.jsonl": "52eda840f872225cc34f8cf12edc2e4621e8a1fef569abf34a50f4a3bd9896f8",
		"testdata/parity/dht/dht_crawler_discovered_nodes.jsonl": "ae6d867378a227284aa0cd93e9120d70afbec1c5e3b19a9f64e09edace4190e0",
	}
	crawlerGetPeersValidateDigests(t, want)
	return want
}

func crawlerGetPeersSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	want := map[string]string{
		"internal/concurrency/batching_channel.go":            "72b3c9fd5fbc8ecbfb0ba2bc2ed5e6c1d45de01f03d3e015b2467f114ec70975",
		"internal/concurrency/buffered_concurrent_channel.go": "4be882800ec66d0c1709319fe029d61773c3f4a37bdb409e3a2f7d5d415d954c",
		"internal/dhtcrawler/config.go":                       "b3cac15378cdca0f21c5f21f37aeb0679815d5bacd16bfa0c3bac2af56db87ef",
		"internal/dhtcrawler/crawler.go":                      "ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6",
		"internal/dhtcrawler/discovered_nodes.go":             "22806cabf39173df71010a54d874a4319458f1715308834be828dbdb99767027",
		"internal/dhtcrawler/factory.go":                      "ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6",
		"internal/dhtcrawler/get_peers.go":                    "c90a41f46c322969188cde55a9e09f64025bc0d7192faca13f50c1bcc8d6cbbf",
		"internal/protocol/id.go":                             "e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f",
		"internal/protocol/dht/client/interface.go":           "477139d727ea685538bccfb0be114ab4fa43556cbdb70d5492a074f24482389f",
		"internal/protocol/dht/ktable/command.go":             "575e58a01856db0746281c3a66a95d6d5483452fb8ab20dc6379ffbc45cedf11",
		"internal/protocol/dht/ktable/hash.go":                "05a350f23a2d21aa5b6fee431b9d065f912c0874b7f145ca59a90cf9dc9b3b8a",
		"internal/protocol/dht/ktable/keyspace.go":            "fe0894e7df90dcfc85b10c72bba3c55d639fff3030735d78172d0b9fdf761573",
		"internal/protocol/dht/ktable/node.go":                "93ed9a76a7cd0f50ee3ad255c6e77a8d19e5fe17081edc6238c5efab4983b3c3",
		"internal/protocol/dht/ktable/query.go":               "103ec27a7904bdbbbd91f3ea1dae1f4d6ea3b3d6652757a6ab8ddbf598a7060e",
		"internal/protocol/dht/ktable/reverse_map.go":         "31e65f7b3b108e13c11772d375f97d7973b00dfc4df490d676a458d4f9a05213",
		"internal/protocol/dht/ktable/table.go":               "68e3caf4394b2692fd9358224cce2b70ae3d90d920097bd28885b6b3bb77848f",
	}
	crawlerGetPeersValidateDigests(t, want)
	return want
}

func crawlerGetPeersValidateDigests(t *testing.T, want map[string]string) {
	t.Helper()
	for path, expected := range want {
		contents, err := os.ReadFile(filepath.Join(crawlerGetPeersRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		actual := fmt.Sprintf("%x", sha256.Sum256(contents))
		if actual != expected {
			t.Fatalf("%s SHA-256 = %s, want %s", path, actual, expected)
		}
	}
}

func crawlerGetPeersRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve get-peers generator source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
}

func reconcileCrawlerGetPeersFixtures(t *testing.T, fixtures []crawlerGetPeersFixture) {
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
	if crawlerGetPeersFixtureSHA256 != "" && actualHash != crawlerGetPeersFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerGetPeersFixtureSHA256)
	}
	path := filepath.Join(crawlerGetPeersRoot(t), "testdata/parity/dht/dht_crawler_get_peers.jsonl")
	if *updateDHTCrawlerGetPeersParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-get-peers-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler get-peers fixture is stale; rerun with -update-dht-crawler-get-peers-parity")
	}
}

var _ concurrency.BufferedConcurrentChannel[nodeHasPeersForHash] = (*crawlerGetPeersManualLane)(nil)
var _ concurrency.BatchingChannel[ktable.Node] = (*crawlerGetPeersDiscovery)(nil)
var _ concurrency.BufferedConcurrentChannel[infoHashWithPeers] = (*crawlerGetPeersMetadataLane)(nil)
var _ client.Client = (*crawlerGetPeersClient)(nil)
var _ ktable.Table = (*crawlerGetPeersTracingTable)(nil)
