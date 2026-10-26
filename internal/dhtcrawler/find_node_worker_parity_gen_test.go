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
	"go/parser"
	"go/token"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

var updateDHTCrawlerFindNodeWorkerParity = flag.Bool(
	"update-dht-crawler-find-node-worker-parity",
	false,
	"rewrite the Rust DHT crawler find-node-worker parity fixture",
)

const crawlerFindNodeWorkerFixtureSHA256 = "e126ad26fd342b14ae0416b3610d991f927dbe9381ac11609ebeba96d67870b7"

var crawlerFindNodeWorkerFixtureIDs = [...]string{
	"production_factory_producer_and_source_contract",
	"find_error_drops_advertised_id",
	"success_ignores_responder_id_and_marks_advertised_node_responded",
	"success_forwards_response_nodes_in_order_after_put",
	"cancelled_after_success_still_puts_then_abandons_blocked_discovery",
	"cancel_after_one_discovery_abandons_blocked_suffix",
	"sought_target_is_read_for_each_callback",
	"lane_error_is_swallowed",
}

type crawlerFindNodeWorkerFixture struct {
	ID        string                        `json:"id"`
	Subsystem string                        `json:"subsystem"`
	Oracle    crawlerFindNodeWorkerOracle   `json:"oracle"`
	Input     crawlerFindNodeWorkerInput    `json:"input"`
	Expected  crawlerFindNodeWorkerExpected `json:"expected"`
}

type crawlerFindNodeWorkerOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Lane        string `json:"lane"`
	Client      string `json:"client"`
	Table       string `json:"table"`
	Discovery   string `json:"discovery"`
}

type crawlerFindNodeWorkerInput struct {
	Kind                  string                            `json:"kind"`
	Nodes                 []crawlerFindNodeWorkerNode       `json:"nodes,omitempty"`
	Outcomes              []crawlerFindNodeWorkerOutcome    `json:"outcomes,omitempty"`
	InitialTarget         string                            `json:"initialTarget,omitempty"`
	TargetsBeforeCallback []string                          `json:"targetsBeforeCallback,omitempty"`
	DiscoveryMode         string                            `json:"discoveryMode,omitempty"`
	DiscoveryCapacity     int                               `json:"discoveryCapacity"`
	CancelBeforeReturn    bool                              `json:"cancelBeforeReturn,omitempty"`
	CancelAfterDeliveries int                               `json:"cancelAfterDeliveries,omitempty"`
	LaneReturnError       bool                              `json:"laneReturnError,omitempty"`
	TableSetup            []crawlerFindNodeWorkerTableSetup `json:"tableSetup,omitempty"`
}

type crawlerFindNodeWorkerNode struct {
	ID          string                           `json:"id"`
	Addr        crawlerPingWorkerAddress         `json:"addr"`
	AddrReturns []crawlerPingWorkerAddress       `json:"addrReturns,omitempty"`
	FreshState  *crawlerFindNodeWorkerFreshState `json:"freshState,omitempty"`
}

type crawlerFindNodeWorkerFreshState struct {
	TimeZero                  bool `json:"timeZero"`
	Dropped                   bool `json:"dropped"`
	SampleInfoHashesCandidate bool `json:"sampleInfohashesCandidate"`
}

type crawlerFindNodeWorkerOutcome struct {
	Kind       string                      `json:"kind"`
	ResponseID string                      `json:"responseId"`
	Nodes      []crawlerFindNodeWorkerNode `json:"nodes"`
}

type crawlerFindNodeWorkerTableSetup struct {
	ID   string                   `json:"id"`
	Addr crawlerPingWorkerAddress `json:"addr"`
}

type crawlerFindNodeWorkerExpected struct {
	NodeCalls        []crawlerFindNodeWorkerNodeCalls `json:"nodeCalls"`
	FindCalls        []crawlerFindNodeWorkerFindCall  `json:"findCalls"`
	SameContext      bool                             `json:"sameContext"`
	BatchCalls       int                              `json:"batchCalls"`
	Commands         []crawlerFindNodeWorkerCommand   `json:"commands"`
	Discoveries      []crawlerFindNodeWorkerNode      `json:"discoveries"`
	Events           []string                         `json:"events"`
	AdvertisedPost   []crawlerFindNodeWorkerTablePost `json:"advertisedPost"`
	ResponseIDPost   []crawlerFindNodeWorkerTablePost `json:"responseIdPost"`
	RunReturned      bool                             `json:"runReturned"`
	ContextCancelled bool                             `json:"contextCancelled"`
	Source           *crawlerFindNodeWorkerSource     `json:"source,omitempty"`
}

type crawlerFindNodeWorkerNodeCalls struct {
	ID                        int `json:"id"`
	Addr                      int `json:"addr"`
	Time                      int `json:"time"`
	Dropped                   int `json:"dropped"`
	SampleInfoHashesCandidate int `json:"sampleInfohashesCandidate"`
}

type crawlerFindNodeWorkerTablePost struct {
	ID              string                    `json:"id"`
	Present         bool                      `json:"present"`
	Addr            *crawlerPingWorkerAddress `json:"addr,omitempty"`
	Responded       bool                      `json:"responded"`
	RetainedDropped bool                      `json:"retainedDropped"`
}

type crawlerFindNodeWorkerFindCall struct {
	Addr   crawlerPingWorkerAddress `json:"addr"`
	Target string                   `json:"target"`
}

type crawlerFindNodeWorkerCommand struct {
	Kind                   string                    `json:"kind"`
	ID                     string                    `json:"id"`
	Addr                   *crawlerPingWorkerAddress `json:"addr,omitempty"`
	OptionCount            int                       `json:"optionCount"`
	Reason                 string                    `json:"reason"`
	ErrorIdentityPreserved bool                      `json:"errorIdentityPreserved"`
	StoredResponded        bool                      `json:"storedResponded"`
}

type crawlerFindNodeWorkerSource struct {
	RunErrorIgnored                  bool              `json:"runErrorIgnored"`
	SharedCallbackContext            bool              `json:"sharedCallbackContext"`
	NoEligibilityRecheck             bool              `json:"noEligibilityRecheck"`
	TargetReadAtEachClientCall       bool              `json:"targetReadAtEachClientCall"`
	ResponseIDIgnored                bool              `json:"responseIdIgnored"`
	ErrorDropsAdvertisedID           bool              `json:"errorDropsAdvertisedId"`
	SuccessUsesNodeRespondedOption   bool              `json:"successUsesNodeRespondedOption"`
	NoPostQueryCancellationBeforePut bool              `json:"noPostQueryCancellationBeforePut"`
	PutPrecedesRecursiveDiscovery    bool              `json:"putPrecedesRecursiveDiscovery"`
	RecursiveDiscoveryBlocksInOrder  bool              `json:"recursiveDiscoveryBlocksInOrder"`
	RecursiveDiscoveryCancelAware    bool              `json:"recursiveDiscoveryCancelAware"`
	ProductionCapacity               int               `json:"productionCapacity"`
	ProductionConcurrency            int               `json:"productionConcurrency"`
	RunDequeuesBeforeAcquire         bool              `json:"runDequeuesBeforeAcquire"`
	RunSpawnsCallbacks               bool              `json:"runSpawnsCallbacks"`
	RunJoinsCallbacks                bool              `json:"runJoinsCallbacks"`
	GenericClosedInputRepeatsReceive bool              `json:"genericClosedInputRepeatsReceive"`
	ClosedInputChecksOpenBoolean     bool              `json:"closedInputChecksOpenBoolean"`
	ClosedInputFindNodeOutcome       string            `json:"closedInputFindNodeOutcome"`
	MaximumRetainedWork              string            `json:"maximumRetainedWork"`
	DefaultScalingFactor             int               `json:"defaultScalingFactor"`
	DiscoveryInputCapacity           int               `json:"discoveryInputCapacity"`
	DiscoveryMaxBatchSize            int               `json:"discoveryMaxBatchSize"`
	DiscoveryBatchIntervalMS         int               `json:"discoveryBatchIntervalMs"`
	DiscoveryOutputCapacity          int               `json:"discoveryOutputCapacity"`
	ProducerInitialQueryBeforeDelay  bool              `json:"producerInitialQueryBeforeDelay"`
	ProducerCutoffSeconds            int               `json:"producerCutoffSeconds"`
	ProducerLimit                    int               `json:"producerLimit"`
	ProducerIntervalMS               int               `json:"producerIntervalMs"`
	ProducerSleepCancellationAware   bool              `json:"producerSleepCancellationAware"`
	EmptyTableCancellationOutcome    string            `json:"emptyTableCancellationOutcome"`
	ProducerEvidenceScope            string            `json:"producerEvidenceScope"`
	SoughtIDRotationSeconds          int               `json:"soughtIdRotationSeconds"`
	SoughtIDInitializedBeforeStart   bool              `json:"soughtIdInitializedBeforeStart"`
	SourceSHA256                     map[string]string `json:"sourceSha256"`
	Evidence                         string            `json:"evidence"`
}

type crawlerFindNodeWorkerScriptedNode struct {
	id    protocol.ID
	addr  netip.AddrPort
	addrs []netip.AddrPort
	calls crawlerFindNodeWorkerNodeCalls
}

func (n *crawlerFindNodeWorkerScriptedNode) ID() protocol.ID {
	n.calls.ID++
	return n.id
}

func (n *crawlerFindNodeWorkerScriptedNode) Addr() netip.AddrPort {
	index := n.calls.Addr
	n.calls.Addr++
	return n.addrAt(index)
}

func (n *crawlerFindNodeWorkerScriptedNode) addrAt(index int) netip.AddrPort {
	if index < len(n.addrs) {
		return n.addrs[index]
	}
	if len(n.addrs) > 0 {
		return n.addrs[len(n.addrs)-1]
	}
	return n.addr
}

func (n *crawlerFindNodeWorkerScriptedNode) Time() time.Time {
	n.calls.Time++
	return time.Time{}
}

func (n *crawlerFindNodeWorkerScriptedNode) Dropped() bool {
	n.calls.Dropped++
	return false
}

func (n *crawlerFindNodeWorkerScriptedNode) IsSampleInfoHashesCandidate() bool {
	n.calls.SampleInfoHashesCandidate++
	return true
}

type crawlerFindNodeWorkerManualLane struct {
	nodes          []*crawlerFindNodeWorkerScriptedNode
	beforeCallback func(int)
	returnErr      error
}

func (*crawlerFindNodeWorkerManualLane) In() chan<- ktable.Node {
	panic("find-node worker must not request the lane sender")
}

func (l *crawlerFindNodeWorkerManualLane) Run(_ context.Context, callback func(ktable.Node)) error {
	for index, node := range l.nodes {
		if l.beforeCallback != nil {
			l.beforeCallback(index)
		}
		callback(node)
	}
	return l.returnErr
}

type crawlerFindNodeWorkerDiscovery struct {
	input chan ktable.Node
}

func (d *crawlerFindNodeWorkerDiscovery) In() chan<- ktable.Node { return d.input }
func (*crawlerFindNodeWorkerDiscovery) Out() <-chan []ktable.Node {
	panic("find-node worker must not request discovered-node output")
}

type crawlerFindNodeWorkerClient struct {
	client.Client
	wantContext        context.Context
	outcomes           []crawlerFindNodeWorkerOutcome
	cancelBeforeReturn context.CancelFunc
	calls              []crawlerFindNodeWorkerFindCall
	sameContext        bool
}

func (c *crawlerFindNodeWorkerClient) FindNode(
	ctx context.Context,
	addr netip.AddrPort,
	target protocol.ID,
) (client.FindNodeResult, error) {
	c.sameContext = c.sameContext && ctx == c.wantContext
	c.calls = append(c.calls, crawlerFindNodeWorkerFindCall{
		Addr: projectCrawlerPingWorkerAddress(addr), Target: target.String(),
	})
	index := len(c.calls) - 1
	outcome := c.outcomes[index]
	if c.cancelBeforeReturn != nil {
		c.cancelBeforeReturn()
		c.cancelBeforeReturn = nil
	}
	nodes := make([]client.NodeInfo, 0, len(outcome.Nodes))
	for _, node := range outcome.Nodes {
		nodes = append(nodes, client.NodeInfo{
			ID: protocol.MustParseID(node.ID), Addr: crawlerFindNodeWorkerAddr(node.Addr),
		})
	}
	result := client.FindNodeResult{
		ID: protocol.MustParseID(outcome.ResponseID), Nodes: nodes,
	}
	if outcome.Kind == "error" {
		return result, crawlerFindNodeWorkerSentinel
	}
	return result, nil
}

var crawlerFindNodeWorkerSentinel = errors.New("oracle find_node failure")

type crawlerFindNodeWorkerEventLog struct {
	mutex  sync.Mutex
	events []string
}

func (l *crawlerFindNodeWorkerEventLog) append(event string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.events = append(l.events, event)
}

func (l *crawlerFindNodeWorkerEventLog) snapshot() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return append([]string{}, l.events...)
}

type crawlerFindNodeWorkerTracingTable struct {
	ktable.Table
	batchCalls int
	commands   []crawlerFindNodeWorkerCommand
	events     *crawlerFindNodeWorkerEventLog
}

func (t *crawlerFindNodeWorkerTracingTable) BatchCommand(commands ...ktable.Command) {
	t.batchCalls++
	start := len(t.commands)
	for _, command := range commands {
		switch command := command.(type) {
		case ktable.PutNode:
			addr := projectCrawlerPingWorkerAddress(command.Addr)
			t.commands = append(t.commands, crawlerFindNodeWorkerCommand{
				Kind: "put_node", ID: command.ID.String(), Addr: &addr,
				OptionCount: len(command.Options),
			})
		case ktable.DropNode:
			t.commands = append(t.commands, crawlerFindNodeWorkerCommand{
				Kind: "drop_node", ID: command.ID.String(), Reason: command.Reason.Error(),
				ErrorIdentityPreserved: errors.Is(command.Reason, crawlerFindNodeWorkerSentinel),
			})
		default:
			panic(fmt.Sprintf("unexpected find-node worker command %T", command))
		}
	}
	t.Table.BatchCommand(commands...)
	for index := start; index < len(t.commands); index++ {
		command := &t.commands[index]
		t.events.append(fmt.Sprintf("table_%s_completed:%s", command.Kind, command.ID))
		if command.Kind != "put_node" {
			continue
		}
		id := protocol.MustParseID(command.ID)
		for _, node := range t.Table.GetClosestNodes(id) {
			if node.ID() == id && !node.Time().IsZero() {
				command.StoredResponded = true
			}
		}
	}
}

type crawlerFindNodeWorkerScenario struct {
	id                    string
	nodes                 []*crawlerFindNodeWorkerScriptedNode
	outcomes              []crawlerFindNodeWorkerOutcome
	targets               []protocol.ID
	discoveryCapacity     int
	discoveryReceiveCount int
	cancelBeforeReturn    bool
	cancelAfterDeliveries int
	laneReturnError       bool
	seedAdvertised        bool
}

func TestGenerateDHTCrawlerFindNodeWorkerParity(t *testing.T) {
	mutableAddress := crawlerFindNodeWorkerScripted(2, "198.51.100.2", 6882)
	mutableAddress.addrs = append(mutableAddress.addrs,
		netip.MustParseAddrPort("198.51.100.22:6982"))
	returned := []crawlerFindNodeWorkerNode{
		makeCrawlerFindNodeWorkerNode(31, "203.0.113.31", 6931, 0),
		makeCrawlerFindNodeWorkerNode(32, "203.0.113.32", 6932, 0),
		makeCrawlerFindNodeWorkerNode(31, "203.0.113.31", 6931, 0),
		makeCrawlerFindNodeWorkerNode(33, "fe80::33", 6933, 7),
	}
	fixtures := []crawlerFindNodeWorkerFixture{
		crawlerFindNodeWorkerSourceFixture(t),
		runCrawlerFindNodeWorkerScenario(t, crawlerFindNodeWorkerScenario{
			id:       "find_error_drops_advertised_id",
			nodes:    []*crawlerFindNodeWorkerScriptedNode{crawlerFindNodeWorkerScripted(1, "198.51.100.1", 6881)},
			outcomes: []crawlerFindNodeWorkerOutcome{crawlerFindNodeWorkerErrorOutcome(221, returned[:2])},
			targets:  []protocol.ID{crawlerPingWorkerID(201)}, seedAdvertised: true,
		}),
		runCrawlerFindNodeWorkerScenario(t, crawlerFindNodeWorkerScenario{
			id:       "success_ignores_responder_id_and_marks_advertised_node_responded",
			nodes:    []*crawlerFindNodeWorkerScriptedNode{mutableAddress},
			outcomes: []crawlerFindNodeWorkerOutcome{crawlerFindNodeWorkerSuccessOutcome(222, nil)},
			targets:  []protocol.ID{crawlerPingWorkerID(202)}, seedAdvertised: true,
		}),
		runCrawlerFindNodeWorkerScenario(t, crawlerFindNodeWorkerScenario{
			id:       "success_forwards_response_nodes_in_order_after_put",
			nodes:    []*crawlerFindNodeWorkerScriptedNode{crawlerFindNodeWorkerScripted(3, "198.51.100.3", 6883)},
			outcomes: []crawlerFindNodeWorkerOutcome{crawlerFindNodeWorkerSuccessOutcome(223, returned)},
			targets:  []protocol.ID{crawlerPingWorkerID(203)}, discoveryReceiveCount: len(returned),
		}),
		runCrawlerFindNodeWorkerScenario(t, crawlerFindNodeWorkerScenario{
			id:       "cancelled_after_success_still_puts_then_abandons_blocked_discovery",
			nodes:    []*crawlerFindNodeWorkerScriptedNode{crawlerFindNodeWorkerScripted(4, "198.51.100.4", 6884)},
			outcomes: []crawlerFindNodeWorkerOutcome{crawlerFindNodeWorkerSuccessOutcome(224, returned)},
			targets:  []protocol.ID{crawlerPingWorkerID(204)}, cancelBeforeReturn: true,
		}),
		runCrawlerFindNodeWorkerScenario(t, crawlerFindNodeWorkerScenario{
			id:       "cancel_after_one_discovery_abandons_blocked_suffix",
			nodes:    []*crawlerFindNodeWorkerScriptedNode{crawlerFindNodeWorkerScripted(5, "198.51.100.5", 6885)},
			outcomes: []crawlerFindNodeWorkerOutcome{crawlerFindNodeWorkerSuccessOutcome(225, returned)},
			targets:  []protocol.ID{crawlerPingWorkerID(205)}, cancelAfterDeliveries: 1,
		}),
		runCrawlerFindNodeWorkerScenario(t, crawlerFindNodeWorkerScenario{
			id: "sought_target_is_read_for_each_callback",
			nodes: []*crawlerFindNodeWorkerScriptedNode{
				crawlerFindNodeWorkerScripted(6, "198.51.100.6", 6886),
				crawlerFindNodeWorkerScripted(7, "198.51.100.7", 6887),
			},
			outcomes: []crawlerFindNodeWorkerOutcome{
				crawlerFindNodeWorkerSuccessOutcome(226, nil),
				crawlerFindNodeWorkerSuccessOutcome(227, nil),
			},
			targets: []protocol.ID{crawlerPingWorkerID(206), crawlerPingWorkerID(207)},
		}),
		runCrawlerFindNodeWorkerScenario(t, crawlerFindNodeWorkerScenario{
			id: "lane_error_is_swallowed", laneReturnError: true,
		}),
	}
	if len(fixtures) != len(crawlerFindNodeWorkerFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerFindNodeWorkerFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerFindNodeWorkerFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerFindNodeWorkerFixtureIDs[index])
		}
	}
	reconcileCrawlerFindNodeWorkerFixtures(t, fixtures)
}

func crawlerFindNodeWorkerSourceFixture(t *testing.T) crawlerFindNodeWorkerFixture {
	t.Helper()
	assertCrawlerFindNodeWorkerSourceShapes(t)
	config := NewDefaultConfig()
	if config.ScalingFactor != 10 {
		t.Fatalf("default scaling factor = %d, want 10", config.ScalingFactor)
	}
	workerCapacity := 10 * int(config.ScalingFactor)
	worker := concurrency.NewBufferedConcurrentChannel[ktable.Node](workerCapacity, workerCapacity)
	if cap(worker.In()) != 100 {
		t.Fatalf("default find-node capacity = %d, want 100", cap(worker.In()))
	}
	discovered := NewDiscoveredNodes(DiscoveredNodesParams{Config: config}).DiscoveredNodes
	value := reflect.ValueOf(discovered).Elem()
	maxBatchSize := int(value.FieldByName("maxBatchSize").Int())
	maxWaitTime := time.Duration(value.FieldByName("maxWaitTime").Int())
	if cap(discovered.In()) != 1000 || cap(discovered.Out()) != 1 ||
		maxBatchSize != 10 || maxWaitTime != 10*time.Millisecond {
		t.Fatalf("default discovery shape = input %d output %d batch %d interval %s",
			cap(discovered.In()), cap(discovered.Out()), maxBatchSize, maxWaitTime)
	}
	return crawlerFindNodeWorkerFixture{
		ID: "production_factory_producer_and_source_contract", Subsystem: "dht_crawler_find_node",
		Oracle: crawlerFindNodeWorkerOracle{
			Composition: "source_factory_and_producer_freshness_gate",
			Determinism: "exact_source_sha256_and_required_ast_shapes",
			Lane:        "production_buffered_concurrent_channel", Client: "production_dht_client_interface",
			Table: "production_ktable_batch_command", Discovery: "production_shared_batching_channel",
		},
		Input: crawlerFindNodeWorkerInput{Kind: "source_contract", DiscoveryCapacity: 1000},
		Expected: crawlerFindNodeWorkerExpected{
			NodeCalls: []crawlerFindNodeWorkerNodeCalls{}, FindCalls: []crawlerFindNodeWorkerFindCall{},
			Commands: []crawlerFindNodeWorkerCommand{}, Discoveries: []crawlerFindNodeWorkerNode{},
			Events: []string{}, AdvertisedPost: []crawlerFindNodeWorkerTablePost{},
			ResponseIDPost: []crawlerFindNodeWorkerTablePost{}, BatchCalls: 0,
			RunReturned: true,
			Source: &crawlerFindNodeWorkerSource{
				RunErrorIgnored: true, SharedCallbackContext: true, NoEligibilityRecheck: true,
				TargetReadAtEachClientCall: true, ResponseIDIgnored: true,
				ErrorDropsAdvertisedID: true, SuccessUsesNodeRespondedOption: true,
				NoPostQueryCancellationBeforePut: true, PutPrecedesRecursiveDiscovery: true,
				RecursiveDiscoveryBlocksInOrder: true, RecursiveDiscoveryCancelAware: true,
				ProductionCapacity: workerCapacity, ProductionConcurrency: workerCapacity,
				RunDequeuesBeforeAcquire: true, RunSpawnsCallbacks: true, RunJoinsCallbacks: false,
				GenericClosedInputRepeatsReceive: true, ClosedInputChecksOpenBoolean: false,
				ClosedInputFindNodeOutcome:      "zero_value_nil_node_callback_panics_on_p.Addr",
				MaximumRetainedWork:             "capacity_plus_concurrency_plus_one_acquire_waiter",
				DefaultScalingFactor:            int(config.ScalingFactor),
				DiscoveryInputCapacity:          cap(discovered.In()),
				DiscoveryMaxBatchSize:           maxBatchSize,
				DiscoveryBatchIntervalMS:        int(maxWaitTime / time.Millisecond),
				DiscoveryOutputCapacity:         cap(discovered.Out()),
				ProducerInitialQueryBeforeDelay: true, ProducerCutoffSeconds: 5,
				ProducerLimit: 10, ProducerIntervalMS: 1000,
				ProducerSleepCancellationAware: false,
				EmptyTableCancellationOutcome:  "continues_query_and_sleep_loop_indefinitely",
				ProducerEvidenceScope:          "ast_and_exact_source_digest_only_not_runtime_executed",
				SoughtIDRotationSeconds:        10, SoughtIDInitializedBeforeStart: true,
				SourceSHA256: crawlerFindNodeWorkerSourceDigests(t),
				Evidence:     "real_runFindNode_rows_plus_exact_Go_AST_and_source_freshness",
			},
		},
	}
}

func runCrawlerFindNodeWorkerScenario(t *testing.T, scenario crawlerFindNodeWorkerScenario) crawlerFindNodeWorkerFixture {
	t.Helper()
	base := ktable.New(ktable.Params{NodeID: protocol.ID{}}).Table
	setup := make([]crawlerFindNodeWorkerTableSetup, 0, len(scenario.nodes))
	retained := make(map[protocol.ID]ktable.Node, len(scenario.nodes))
	if scenario.seedAdvertised {
		for _, node := range scenario.nodes {
			base.PutNode(node.id, node.addr)
			retained[node.id] = crawlerFindNodeWorkerTableNode(base, node.id)
			setup = append(setup, crawlerFindNodeWorkerTableSetup{
				ID: node.id.String(), Addr: projectCrawlerPingWorkerAddress(node.addr),
			})
		}
	}
	events := &crawlerFindNodeWorkerEventLog{}
	tracingTable := &crawlerFindNodeWorkerTracingTable{Table: base, events: events}
	discovery := &crawlerFindNodeWorkerDiscovery{input: make(chan ktable.Node, scenario.discoveryCapacity)}
	sought := &concurrency.AtomicValue[protocol.ID]{}
	if len(scenario.targets) > 0 {
		sought.Set(scenario.targets[0])
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	findClient := &crawlerFindNodeWorkerClient{
		wantContext: ctx, outcomes: scenario.outcomes, sameContext: true,
	}
	if scenario.cancelBeforeReturn {
		findClient.cancelBeforeReturn = cancel
	}
	lane := &crawlerFindNodeWorkerManualLane{
		nodes: scenario.nodes,
		beforeCallback: func(index int) {
			if index < len(scenario.targets) {
				sought.Set(scenario.targets[index])
			}
		},
	}
	if scenario.laneReturnError {
		lane.returnErr = errors.New("oracle lane error")
	}
	c := crawler{
		kTable: tracingTable, client: findClient, nodesForFindNode: lane,
		discoveredNodes: discovery, soughtNodeID: sought,
	}
	discoveries := make([]crawlerFindNodeWorkerNode, 0)
	if scenario.cancelAfterDeliveries > 0 {
		done := make(chan struct{})
		go func() { c.runFindNode(ctx); close(done) }()
		for range scenario.cancelAfterDeliveries {
			select {
			case node := <-discovery.input:
				projected := crawlerFindNodeWorkerProjectNode(node)
				discoveries = append(discoveries, projected)
				events.append("discovery_accepted:" + projected.ID)
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for recursive discovery prefix")
			}
		}
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("find-node worker did not return after suffix cancellation")
		}
	} else if scenario.discoveryReceiveCount > 0 {
		received := make(chan []crawlerFindNodeWorkerNode, 1)
		go func() {
			collected := make([]crawlerFindNodeWorkerNode, 0, scenario.discoveryReceiveCount)
			for range scenario.discoveryReceiveCount {
				projected := crawlerFindNodeWorkerProjectNode(<-discovery.input)
				collected = append(collected, projected)
				events.append("discovery_accepted:" + projected.ID)
			}
			received <- collected
		}()
		c.runFindNode(ctx)
		select {
		case discoveries = <-received:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out collecting recursive discoveries")
		}
	} else {
		c.runFindNode(ctx)
		for len(discovery.input) > 0 {
			projected := crawlerFindNodeWorkerProjectNode(<-discovery.input)
			discoveries = append(discoveries, projected)
			events.append("discovery_accepted:" + projected.ID)
		}
	}
	nodeCalls := make([]crawlerFindNodeWorkerNodeCalls, 0, len(scenario.nodes))
	inputNodes := make([]crawlerFindNodeWorkerNode, 0, len(scenario.nodes))
	for _, node := range scenario.nodes {
		nodeCalls = append(nodeCalls, node.calls)
		addrReturns := make([]crawlerPingWorkerAddress, 0, node.calls.Addr)
		for index := range node.calls.Addr {
			addrReturns = append(addrReturns,
				projectCrawlerPingWorkerAddress(node.addrAt(index)))
		}
		inputNodes = append(inputNodes, crawlerFindNodeWorkerNode{
			ID: node.id.String(), Addr: projectCrawlerPingWorkerAddress(node.addr),
			AddrReturns: addrReturns,
		})
	}
	targets := make([]string, 0, len(scenario.targets))
	for _, target := range scenario.targets {
		targets = append(targets, target.String())
	}
	mode := "buffered"
	if scenario.discoveryCapacity == 0 {
		mode = "unbuffered"
	}
	input := crawlerFindNodeWorkerInput{
		Kind: "run_find_node", Nodes: inputNodes, Outcomes: scenario.outcomes,
		DiscoveryMode: mode, DiscoveryCapacity: scenario.discoveryCapacity,
		CancelBeforeReturn:    scenario.cancelBeforeReturn,
		CancelAfterDeliveries: scenario.cancelAfterDeliveries,
		LaneReturnError:       scenario.laneReturnError, TableSetup: setup,
	}
	if len(targets) > 0 {
		input.InitialTarget = targets[0]
		input.TargetsBeforeCallback = targets
	}
	advertisedPost := make([]crawlerFindNodeWorkerTablePost, 0, len(scenario.nodes))
	for _, node := range scenario.nodes {
		current := crawlerFindNodeWorkerTableNode(base, node.id)
		handle := retained[node.id]
		if handle == nil {
			handle = current
		}
		advertisedPost = append(advertisedPost,
			crawlerFindNodeWorkerTablePostState(node.id, current, handle))
	}
	responsePost := make([]crawlerFindNodeWorkerTablePost, 0, len(scenario.outcomes))
	for _, outcome := range scenario.outcomes {
		id := protocol.MustParseID(outcome.ResponseID)
		current := crawlerFindNodeWorkerTableNode(base, id)
		responsePost = append(responsePost,
			crawlerFindNodeWorkerTablePostState(id, current, current))
	}
	return crawlerFindNodeWorkerFixture{
		ID: scenario.id, Subsystem: "dht_crawler_find_node",
		Oracle: crawlerFindNodeWorkerOracle{
			Composition: "actual_crawler_runFindNode_with_manual_callback_lane",
			Determinism: "synchronous_callbacks_scripted_client_and_capacity_controlled_discovery",
			Lane:        "manual_ordered_callback_interface_implementation",
			Client:      "scripted_client_Client_findNode_override",
			Table:       "tracing_wrapper_over_actual_ktable",
			Discovery:   "manual_batching_channel_input_with_explicit_capacity",
		},
		Input: input,
		Expected: crawlerFindNodeWorkerExpected{
			NodeCalls: nodeCalls, FindCalls: append([]crawlerFindNodeWorkerFindCall{}, findClient.calls...),
			SameContext: len(findClient.calls) > 0 && findClient.sameContext,
			BatchCalls:  tracingTable.batchCalls,
			Commands:    append([]crawlerFindNodeWorkerCommand{}, tracingTable.commands...),
			Discoveries: discoveries, RunReturned: true, ContextCancelled: ctx.Err() != nil,
			Events: events.snapshot(), AdvertisedPost: advertisedPost, ResponseIDPost: responsePost,
		},
	}
}

func crawlerFindNodeWorkerScripted(value int, ip string, port uint16) *crawlerFindNodeWorkerScriptedNode {
	addr := netip.MustParseAddrPort(fmt.Sprintf("%s:%d", ip, port))
	return &crawlerFindNodeWorkerScriptedNode{
		id: crawlerPingWorkerID(value), addr: addr, addrs: []netip.AddrPort{addr},
	}
}

func makeCrawlerFindNodeWorkerNode(value int, ip string, port uint16, scope uint32) crawlerFindNodeWorkerNode {
	addr := netip.MustParseAddr(ip)
	if scope != 0 {
		addr = addr.WithZone(fmt.Sprint(scope))
	}
	return crawlerFindNodeWorkerNode{
		ID:   crawlerPingWorkerID(value).String(),
		Addr: projectCrawlerPingWorkerAddress(netip.AddrPortFrom(addr, port)),
	}
}

func crawlerFindNodeWorkerSuccessOutcome(responseID int, nodes []crawlerFindNodeWorkerNode) crawlerFindNodeWorkerOutcome {
	if nodes == nil {
		nodes = []crawlerFindNodeWorkerNode{}
	}
	return crawlerFindNodeWorkerOutcome{Kind: "success", ResponseID: crawlerPingWorkerID(responseID).String(), Nodes: nodes}
}

func crawlerFindNodeWorkerErrorOutcome(responseID int, nodes []crawlerFindNodeWorkerNode) crawlerFindNodeWorkerOutcome {
	return crawlerFindNodeWorkerOutcome{
		Kind: "error", ResponseID: crawlerPingWorkerID(responseID).String(), Nodes: nodes,
	}
}

func crawlerFindNodeWorkerAddr(addr crawlerPingWorkerAddress) netip.AddrPort {
	parsed := netip.MustParseAddr(addr.IP)
	if addr.Scope != 0 {
		parsed = parsed.WithZone(fmt.Sprint(addr.Scope))
	}
	return netip.AddrPortFrom(parsed, addr.Port)
}

func crawlerFindNodeWorkerProjectNode(node ktable.Node) crawlerFindNodeWorkerNode {
	return crawlerFindNodeWorkerNode{
		ID: node.ID().String(), Addr: projectCrawlerPingWorkerAddress(node.Addr()),
		FreshState: &crawlerFindNodeWorkerFreshState{
			TimeZero: node.Time().IsZero(), Dropped: node.Dropped(),
			SampleInfoHashesCandidate: node.IsSampleInfoHashesCandidate(),
		},
	}
}

func crawlerFindNodeWorkerTableNode(table ktable.Table, id protocol.ID) ktable.Node {
	for _, node := range table.GetClosestNodes(id) {
		if node.ID() == id {
			return node
		}
	}
	return nil
}

func crawlerFindNodeWorkerTablePostState(
	id protocol.ID,
	current ktable.Node,
	retained ktable.Node,
) crawlerFindNodeWorkerTablePost {
	state := crawlerFindNodeWorkerTablePost{ID: id.String(), Present: current != nil}
	if current != nil {
		addr := projectCrawlerPingWorkerAddress(current.Addr())
		state.Addr = &addr
		state.Responded = !current.Time().IsZero()
	}
	if retained != nil {
		state.RetainedDropped = retained.Dropped()
	}
	return state
}

func assertCrawlerFindNodeWorkerSourceShapes(t *testing.T) {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	findSet, find := crawlerPingWorkerParseFunc(t, filepath.Join(root, "internal/dhtcrawler/find_node.go"), "runFindNode")
	wantRunSet, wantRun := crawlerFindNodeWorkerParseSourceFunc(t, `package dhtcrawler
func (c *crawler) runFindNode(ctx context.Context) {
	_ = c.nodesForFindNode.Run(ctx, func(p ktable.Node) {
		res, err := c.client.FindNode(ctx, p.Addr(), c.soughtNodeID.Get())
		if err != nil {
			c.kTable.BatchCommand(ktable.DropNode{ID: p.ID(), Reason: fmt.Errorf("find_node failed: %w", err),})
		} else {
			c.kTable.BatchCommand(ktable.PutNode{ID: p.ID(), Addr: p.Addr(), Options: []ktable.NodeOption{ktable.NodeResponded()},})
			for _, n := range res.Nodes {
				select {
				case <-ctx.Done(): return
				case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr): continue
				}
			}
		}
	})
}`)
	crawlerFindNodeWorkerAssertBody(t, findSet, find, wantRunSet, wantRun, "runFindNode")
	producerSet, producer := crawlerPingWorkerParseFunc(t, filepath.Join(root, "internal/dhtcrawler/find_node.go"), "getNodesForFindNode")
	wantProducerSet, wantProducer := crawlerFindNodeWorkerParseSourceFunc(t, `package dhtcrawler
func (c *crawler) getNodesForFindNode(ctx context.Context) {
	for {
		peers := c.kTable.GetOldestNodes(time.Now().Add(-(5 * time.Second)), 10)
		for _, p := range peers {
			select {
			case <-ctx.Done(): return
			case c.nodesForFindNode.In() <- p: continue
			}
		}
		<-time.After(time.Second)
	}
}`)
	crawlerFindNodeWorkerAssertBody(t, producerSet, producer, wantProducerSet, wantProducer, "getNodesForFindNode")

	factorySet, factory := crawlerPingWorkerParseFunc(t, filepath.Join(root, "internal/dhtcrawler/factory.go"), "New")
	values := make(map[string]ast.Expr)
	ast.Inspect(factory.Body, func(node ast.Node) bool {
		entry, ok := node.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		key, ok := entry.Key.(*ast.Ident)
		if ok {
			values[key.Name] = entry.Value
		}
		return true
	})
	crawlerPingWorkerAssertExpr(t, factorySet, values["nodesForFindNode"], "concurrency.NewBufferedConcurrentChannel[ktable.Node](10*scalingFactor, 10*scalingFactor)")
	crawlerPingWorkerAssertExpr(t, factorySet, values["soughtNodeID"], "&concurrency.AtomicValue[protocol.ID]{}")
	factoryText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, factorySet, factory.Body))
	setTarget := crawlerPingWorkerTokenText("c.soughtNodeID.Set(protocol.RandomNodeID())")
	startCrawler := crawlerPingWorkerTokenText("go c.start()")
	setIndex := strings.Index(factoryText, setTarget)
	startIndex := strings.Index(factoryText, startCrawler)
	if setIndex < 0 || startIndex < 0 || setIndex >= startIndex {
		t.Fatal("factory no longer initializes the sought ID before starting the crawler")
	}
	crawlerSet, start := crawlerPingWorkerParseFunc(t, filepath.Join(root, "internal/dhtcrawler/crawler.go"), "start")
	startText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, crawlerSet, start.Body))
	for _, required := range []string{"go c.rotateSoughtNodeID(ctx)", "go c.runFindNode(ctx)", "go c.getNodesForFindNode(ctx)"} {
		if !bytes.Contains([]byte(startText), []byte(crawlerPingWorkerTokenText(required))) {
			t.Fatalf("crawler start missing %s", required)
		}
	}
	rotateSet, rotate := crawlerPingWorkerParseFunc(t, filepath.Join(root, "internal/dhtcrawler/crawler.go"), "rotateSoughtNodeID")
	rotateText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, rotateSet, rotate.Body))
	for _, required := range []string{"case <-time.After(10 * time.Second):", "c.soughtNodeID.Set(protocol.RandomNodeID())"} {
		if !bytes.Contains([]byte(rotateText), []byte(crawlerPingWorkerTokenText(required))) {
			t.Fatalf("target rotation missing %s", required)
		}
	}
	channelSet, channelRun := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/concurrency/buffered_concurrent_channel.go"), "Run")
	wantChannelSet, wantChannelRun := crawlerFindNodeWorkerParseSourceFunc(t, `package concurrency
func (ch bufferedConcurrentChannel[T]) Run(ctx context.Context, f func(T)) error {
	for {
		select {
		case <-ctx.Done(): return ctx.Err()
		case next := <-ch.ch:
			if err := ch.sem.Acquire(ctx, 1); err != nil { return err }
			go func() {
				defer ch.sem.Release(1)
				f(next)
			}()
		}
	}
}`)
	crawlerFindNodeWorkerAssertBody(t, channelSet, channelRun,
		wantChannelSet, wantChannelRun, "BufferedConcurrentChannel.Run")
}

func crawlerFindNodeWorkerParseSourceFunc(t *testing.T, source string) (*token.FileSet, *ast.FuncDecl) {
	t.Helper()
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "expected.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok {
			return set, function
		}
	}
	t.Fatal("expected source function missing")
	return nil, nil
}

func crawlerFindNodeWorkerAssertBody(t *testing.T, gotSet *token.FileSet, got *ast.FuncDecl, wantSet *token.FileSet, want *ast.FuncDecl, name string) {
	t.Helper()
	gotText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, gotSet, got.Body))
	wantText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, wantSet, want.Body))
	gotText = strings.ReplaceAll(gotText, ",:\x00}:\x00", "}:\x00")
	wantText = strings.ReplaceAll(wantText, ",:\x00}:\x00", "}:\x00")
	if gotText != wantText {
		t.Fatalf("%s AST body changed\ngot: %q\nwant: %q", name, gotText, wantText)
	}
}

func crawlerFindNodeWorkerSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	paths := []string{
		"internal/concurrency/atomic.go", "internal/concurrency/batching_channel.go",
		"internal/concurrency/buffered_concurrent_channel.go",
		"internal/dhtcrawler/config.go", "internal/dhtcrawler/crawler.go",
		"internal/dhtcrawler/discovered_nodes.go", "internal/dhtcrawler/factory.go",
		"internal/dhtcrawler/find_node.go", "internal/protocol/dht/client/interface.go",
		"internal/protocol/dht/ktable/command.go", "internal/protocol/dht/ktable/node.go",
		"internal/protocol/dht/ktable/query.go", "internal/protocol/dht/ktable/table.go",
	}
	digests := make(map[string]string, len(paths))
	for _, name := range paths {
		contents, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(contents)
		digests[name] = fmt.Sprintf("%x", digest)
	}
	return digests
}

func reconcileCrawlerFindNodeWorkerFixtures(t *testing.T, fixtures []crawlerFindNodeWorkerFixture) {
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
	digest := sha256.Sum256(encoded.Bytes())
	actualHash := fmt.Sprintf("%x", digest)
	if crawlerFindNodeWorkerFixtureSHA256 != "" && actualHash != crawlerFindNodeWorkerFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerFindNodeWorkerFixtureSHA256)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve find-node worker generator source")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(source), "../../testdata/parity/dht/dht_crawler_find_node_worker.jsonl"))
	if *updateDHTCrawlerFindNodeWorkerParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-find-node-worker-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler find-node-worker fixture is stale; rerun with -update-dht-crawler-find-node-worker-parity")
	}
}
