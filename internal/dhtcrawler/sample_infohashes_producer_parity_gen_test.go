package dhtcrawler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

var updateDHTCrawlerSampleInfoHashesProducerParity = flag.Bool(
	"update-dht-crawler-sample-infohashes-producer-parity",
	false,
	"rewrite the Rust DHT crawler sample-infohashes-producer parity fixture",
)

const crawlerSampleInfoHashesProducerFixtureSHA256 = "b0069a060b32edc4e1c6f5b2008f6b50f796eea6d162b4df3a148cad29745c1e"

var crawlerSampleInfoHashesProducerFixtureIDs = [...]string{
	"production_source_factory_and_lifecycle_contract",
	"already_cancelled_still_queries_before_first_send",
	"ordered_prefix_then_cancel_at_blocked_third_send",
}

type crawlerSampleInfoHashesProducerFixture struct {
	ID             string                                  `json:"id"`
	Subsystem      string                                  `json:"subsystem"`
	Classification string                                  `json:"classification"`
	Oracle         crawlerSampleInfoHashesProducerOracle   `json:"oracle"`
	Input          crawlerSampleInfoHashesProducerInput    `json:"input"`
	Expected       crawlerSampleInfoHashesProducerExpected `json:"expected"`
}

type crawlerSampleInfoHashesProducerOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Table       string `json:"table"`
	Lane        string `json:"lane"`
	Clock       string `json:"clock"`
}

type crawlerSampleInfoHashesProducerInput struct {
	Kind                      string                                `json:"kind"`
	ContextInitiallyCancelled bool                                  `json:"contextInitiallyCancelled"`
	Nodes                     []crawlerSampleInfoHashesProducerNode `json:"nodes"`
	LaneCapacity              int                                   `json:"laneCapacity"`
	CancelAtLaneInCall        int                                   `json:"cancelAtLaneInCall"`
}

type crawlerSampleInfoHashesProducerExpected struct {
	GetCalls         []crawlerSampleInfoHashesProducerGetCall       `json:"getCalls"`
	LaneInCalls      int                                            `json:"laneInCalls"`
	Deliveries       []crawlerSampleInfoHashesProducerDelivery      `json:"deliveries"`
	Abandoned        []crawlerSampleInfoHashesProducerNode          `json:"abandoned"`
	AccessorCalls    []crawlerSampleInfoHashesProducerAccessorCalls `json:"accessorCalls"`
	Events           []string                                       `json:"events"`
	RunReturned      bool                                           `json:"runReturned"`
	ContextCancelled bool                                           `json:"contextCancelled"`
	Source           *crawlerSampleInfoHashesProducerSource         `json:"source,omitempty"`
}

type crawlerSampleInfoHashesProducerGetCall struct {
	Limit int `json:"limit"`
}

type crawlerSampleInfoHashesProducerDelivery struct {
	Node                  crawlerSampleInfoHashesProducerNode `json:"node"`
	SameGoInterfaceHandle bool                                `json:"sameGoInterfaceHandle"`
}

type crawlerSampleInfoHashesProducerNode struct {
	Token string `json:"token"`
	ID    string `json:"id"`
	Addr  string `json:"addr"`
}

type crawlerSampleInfoHashesProducerAccessorCalls struct {
	Token                     string `json:"token"`
	ID                        int    `json:"id"`
	Addr                      int    `json:"addr"`
	Time                      int    `json:"time"`
	Dropped                   int    `json:"dropped"`
	SampleInfoHashesCandidate int    `json:"sampleInfohashesCandidate"`
}

type crawlerSampleInfoHashesProducerSource struct {
	ImmediateFirstQuery                     bool              `json:"immediateFirstQuery"`
	Limit                                   int               `json:"limit"`
	ProductionSelectionOrder                string            `json:"productionSelectionOrder"`
	PreservesReturnedOrder                  bool              `json:"preservesReturnedOrder"`
	PreservesReturnedHandleIdentity         bool              `json:"preservesReturnedHandleIdentity"`
	GoLaneElementType                       string            `json:"goLaneElementType"`
	ProducerOutputProvenance                string            `json:"producerOutputProvenance"`
	SharedLaneAlsoReceivesDiscoveredNodes   bool              `json:"sharedLaneAlsoReceivesDiscoveredNodes"`
	GoLaneHasExplicitSourceTag              bool              `json:"goLaneHasExplicitSourceTag"`
	SelectOperandsEvaluatedBeforeChoice     bool              `json:"selectOperandsEvaluatedBeforeChoice"`
	LaneInEvaluatedWhenCancelWins           bool              `json:"laneInEvaluatedWhenCancelWins"`
	PerNodeSendCancellationAware            bool              `json:"perNodeSendCancellationAware"`
	NoNodeProjectionOrRecheck               bool              `json:"noNodeProjectionOrRecheck"`
	PostBatchDelayMS                        int               `json:"postBatchDelayMs"`
	PostBatchSleepCancellationAware         bool              `json:"postBatchSleepCancellationAware"`
	EmptyTableCancellationOutcome           string            `json:"emptyTableCancellationOutcome"`
	ReadySendCancelOutcome                  string            `json:"readySendCancelOutcome"`
	ProducerDetached                        bool              `json:"producerDetached"`
	ProducerJoined                          bool              `json:"producerJoined"`
	SharedLaneWithSampleWorker              bool              `json:"sharedLaneWithSampleWorker"`
	ProductionCapacity                      int               `json:"productionCapacity"`
	ProductionConcurrency                   int               `json:"productionConcurrency"`
	ProductionCapacityIsTotalRetentionBound bool              `json:"productionCapacityIsTotalRetentionBound"`
	ConsumerDequeuesBeforeSemaphore         bool              `json:"consumerDequeuesBeforeSemaphore"`
	ConsumerCallbacksDetached               bool              `json:"consumerCallbacksDetached"`
	RuntimeLaneGateIsOracleOnly             bool              `json:"runtimeLaneGateIsOracleOnly"`
	PostBatchDelayRuntimeObserved           bool              `json:"postBatchDelayRuntimeObserved"`
	EmptyTableRuntimeObserved               bool              `json:"emptyTableRuntimeObserved"`
	RuntimeRowsReturnBeforeSleep            bool              `json:"runtimeRowsReturnBeforeSleep"`
	SourceSHA256                            map[string]string `json:"sourceSha256"`
	Evidence                                string            `json:"evidence"`
}

type crawlerSampleInfoHashesProducerEventLog struct {
	mutex  sync.Mutex
	events []string
}

func (l *crawlerSampleInfoHashesProducerEventLog) append(event string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.events = append(l.events, event)
}

func (l *crawlerSampleInfoHashesProducerEventLog) snapshot() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return append([]string{}, l.events...)
}

type crawlerSampleInfoHashesProducerProbeNode struct {
	token      string
	id         protocol.ID
	addr       netip.AddrPort
	idCalls    int
	addrCalls  int
	timeCalls  int
	dropCalls  int
	beP51Calls int
}

func (n *crawlerSampleInfoHashesProducerProbeNode) ID() protocol.ID {
	n.idCalls++
	return n.id
}

func (n *crawlerSampleInfoHashesProducerProbeNode) Addr() netip.AddrPort {
	n.addrCalls++
	return n.addr
}

func (n *crawlerSampleInfoHashesProducerProbeNode) Time() time.Time {
	n.timeCalls++
	return time.Time{}
}

func (n *crawlerSampleInfoHashesProducerProbeNode) Dropped() bool {
	n.dropCalls++
	return false
}

func (n *crawlerSampleInfoHashesProducerProbeNode) IsSampleInfoHashesCandidate() bool {
	n.beP51Calls++
	return true
}

func (n *crawlerSampleInfoHashesProducerProbeNode) assertUntouched(t *testing.T) {
	t.Helper()
	if n.idCalls != 0 || n.addrCalls != 0 || n.timeCalls != 0 || n.dropCalls != 0 || n.beP51Calls != 0 {
		t.Fatalf("node %s accessor calls = id:%d addr:%d time:%d dropped:%d bep51:%d, want all zero",
			n.token, n.idCalls, n.addrCalls, n.timeCalls, n.dropCalls, n.beP51Calls)
	}
}

func (n *crawlerSampleInfoHashesProducerProbeNode) fixtureNode() crawlerSampleInfoHashesProducerNode {
	return crawlerSampleInfoHashesProducerNode{Token: n.token, ID: n.id.String(), Addr: n.addr.String()}
}

func (n *crawlerSampleInfoHashesProducerProbeNode) fixtureAccessorCalls() crawlerSampleInfoHashesProducerAccessorCalls {
	return crawlerSampleInfoHashesProducerAccessorCalls{
		Token: n.token, ID: n.idCalls, Addr: n.addrCalls, Time: n.timeCalls,
		Dropped: n.dropCalls, SampleInfoHashesCandidate: n.beP51Calls,
	}
}

type crawlerSampleInfoHashesProducerTable struct {
	ktable.Table
	mutex  sync.Mutex
	nodes  []ktable.Node
	events *crawlerSampleInfoHashesProducerEventLog
	calls  []crawlerSampleInfoHashesProducerGetCall
}

func (t *crawlerSampleInfoHashesProducerTable) GetNodesForSampleInfoHashes(limit int) []ktable.Node {
	call := crawlerSampleInfoHashesProducerGetCall{Limit: limit}
	t.mutex.Lock()
	t.calls = append(t.calls, call)
	t.mutex.Unlock()
	t.events.append("get_nodes_for_sample_infohashes")
	return append([]ktable.Node{}, t.nodes...)
}

func (t *crawlerSampleInfoHashesProducerTable) snapshotCalls() []crawlerSampleInfoHashesProducerGetCall {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return append([]crawlerSampleInfoHashesProducerGetCall{}, t.calls...)
}

type crawlerSampleInfoHashesProducerLane struct {
	input   chan ktable.Node
	entered chan int
	gateAt  map[int]<-chan struct{}
	events  *crawlerSampleInfoHashesProducerEventLog
	mutex   sync.Mutex
	calls   int
}

func (l *crawlerSampleInfoHashesProducerLane) In() chan<- ktable.Node {
	l.mutex.Lock()
	l.calls++
	call := l.calls
	l.mutex.Unlock()
	l.events.append(fmt.Sprintf("lane_in:%d", call))
	l.entered <- call
	if gate := l.gateAt[call]; gate != nil {
		<-gate
	}
	return l.input
}

func (*crawlerSampleInfoHashesProducerLane) Run(context.Context, func(ktable.Node)) error {
	panic("sample-infohashes producer oracle must not run the consumer lane")
}

func (l *crawlerSampleInfoHashesProducerLane) callCount() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.calls
}

func TestGenerateDHTCrawlerSampleInfoHashesProducerParity(t *testing.T) {
	fixtures := []crawlerSampleInfoHashesProducerFixture{
		crawlerSampleInfoHashesProducerSourceFixture(t),
		runCrawlerSampleInfoHashesProducerAlreadyCancelled(t),
		runCrawlerSampleInfoHashesProducerOrderedPrefix(t),
	}
	if len(fixtures) != len(crawlerSampleInfoHashesProducerFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerSampleInfoHashesProducerFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerSampleInfoHashesProducerFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerSampleInfoHashesProducerFixtureIDs[index])
		}
		if fixture.Subsystem != "dht_crawler_sample_infohashes_producer" {
			t.Fatalf("fixture %s subsystem = %q", fixture.ID, fixture.Subsystem)
		}
	}
	if fixtures[0].Classification != "SOURCE_ONLY" ||
		fixtures[1].Classification != "RUNTIME_EXACT" ||
		fixtures[2].Classification != "RUNTIME_EXACT" {
		t.Fatal("sample-infohashes producer fixture classifications drifted")
	}
	reconcileCrawlerSampleInfoHashesProducerFixtures(t, fixtures)
}

func crawlerSampleInfoHashesProducerSourceFixture(t *testing.T) crawlerSampleInfoHashesProducerFixture {
	t.Helper()
	assertCrawlerSampleInfoHashesProducerSourceShapes(t)
	scaling := int(NewDefaultConfig().ScalingFactor)
	if scaling != 10 {
		t.Fatalf("default scaling factor = %d, want 10", scaling)
	}
	return crawlerSampleInfoHashesProducerFixture{
		ID:             "production_source_factory_and_lifecycle_contract",
		Subsystem:      "dht_crawler_sample_infohashes_producer",
		Classification: "SOURCE_ONLY",
		Oracle: crawlerSampleInfoHashesProducerOracle{
			Composition: "exact_production_source_factory_and_lifecycle_shapes",
			Determinism: "normalized_ast_and_whole_source_sha256",
			Table:       "production_ktable_Table_GetNodesForSampleInfoHashes_interface",
			Lane:        "production_buffered_concurrent_channel",
			Clock:       "exact_source_unconditional_time_After_after_each_round",
		},
		Input: crawlerSampleInfoHashesProducerInput{
			Kind: "source_contract", Nodes: []crawlerSampleInfoHashesProducerNode{},
		},
		Expected: crawlerSampleInfoHashesProducerExpected{
			GetCalls: []crawlerSampleInfoHashesProducerGetCall{}, Deliveries: []crawlerSampleInfoHashesProducerDelivery{},
			Abandoned:     []crawlerSampleInfoHashesProducerNode{},
			AccessorCalls: []crawlerSampleInfoHashesProducerAccessorCalls{},
			Events:        []string{}, RunReturned: false,
			Source: &crawlerSampleInfoHashesProducerSource{
				ImmediateFirstQuery:                     true,
				Limit:                                   60,
				ProductionSelectionOrder:                "unspecified_map_iteration_prefix",
				PreservesReturnedOrder:                  true,
				PreservesReturnedHandleIdentity:         true,
				GoLaneElementType:                       "ktable.Node",
				ProducerOutputProvenance:                "retained_table_handle",
				SharedLaneAlsoReceivesDiscoveredNodes:   true,
				GoLaneHasExplicitSourceTag:              false,
				SelectOperandsEvaluatedBeforeChoice:     true,
				LaneInEvaluatedWhenCancelWins:           true,
				PerNodeSendCancellationAware:            true,
				NoNodeProjectionOrRecheck:               true,
				PostBatchDelayMS:                        1000,
				PostBatchSleepCancellationAware:         false,
				EmptyTableCancellationOutcome:           "while_every_query_remains_empty_queries_then_unconditionally_sleeps_one_second_forever_without_observing_cancellation",
				ReadySendCancelOutcome:                  "go_select_chooses_nondeterministically_when_send_and_cancellation_are_both_ready",
				ProducerDetached:                        true,
				ProducerJoined:                          false,
				SharedLaneWithSampleWorker:              true,
				ProductionCapacity:                      10 * scaling,
				ProductionConcurrency:                   10 * scaling,
				ProductionCapacityIsTotalRetentionBound: false,
				ConsumerDequeuesBeforeSemaphore:         true,
				ConsumerCallbacksDetached:               true,
				RuntimeLaneGateIsOracleOnly:             true,
				PostBatchDelayRuntimeObserved:           false,
				EmptyTableRuntimeObserved:               false,
				RuntimeRowsReturnBeforeSleep:            true,
				SourceSHA256:                            crawlerSampleInfoHashesProducerSourceDigests(t),
				Evidence:                                "runtime rows call the actual producer and return during its per-node select; the manual third-In gate only makes cancellation deterministic, while post-batch timing and perpetual-empty cancellation remain source-only",
			},
		},
	}
}

func runCrawlerSampleInfoHashesProducerAlreadyCancelled(t *testing.T) crawlerSampleInfoHashesProducerFixture {
	t.Helper()
	events := &crawlerSampleInfoHashesProducerEventLog{}
	node := newCrawlerSampleInfoHashesProducerProbeNode("A", 1)
	lane := &crawlerSampleInfoHashesProducerLane{
		input: make(chan ktable.Node), entered: make(chan int, 4),
		gateAt: map[int]<-chan struct{}{}, events: events,
	}
	table := &crawlerSampleInfoHashesProducerTable{
		nodes: []ktable.Node{node}, events: events,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := crawler{kTable: table, nodesForSampleInfoHashes: lane}
	c.getNodesForSampleInfoHashes(ctx)
	events.append("return")

	calls := table.snapshotCalls()
	if len(calls) != 1 || calls[0].Limit != 60 {
		t.Fatalf("already-cancelled table calls = %+v, want one limit-60 call", calls)
	}
	if lane.callCount() != 1 || len(lane.input) != 0 {
		t.Fatalf("already-cancelled lane calls/queued = %d/%d, want 1/0", lane.callCount(), len(lane.input))
	}
	node.assertUntouched(t)
	return crawlerSampleInfoHashesProducerFixture{
		ID:        "already_cancelled_still_queries_before_first_send",
		Subsystem: "dht_crawler_sample_infohashes_producer", Classification: "RUNTIME_EXACT",
		Oracle: crawlerSampleInfoHashesProducerRuntimeOracle("pre_cancelled_context_and_unbuffered_lane"),
		Input: crawlerSampleInfoHashesProducerInput{
			Kind: "actual_getNodesForSampleInfoHashes", ContextInitiallyCancelled: true,
			Nodes: []crawlerSampleInfoHashesProducerNode{node.fixtureNode()}, LaneCapacity: 0,
		},
		Expected: crawlerSampleInfoHashesProducerExpected{
			GetCalls: calls, LaneInCalls: 1,
			Deliveries:    []crawlerSampleInfoHashesProducerDelivery{},
			Abandoned:     []crawlerSampleInfoHashesProducerNode{node.fixtureNode()},
			AccessorCalls: []crawlerSampleInfoHashesProducerAccessorCalls{node.fixtureAccessorCalls()},
			Events:        events.snapshot(), RunReturned: true, ContextCancelled: true,
		},
	}
}

func runCrawlerSampleInfoHashesProducerOrderedPrefix(t *testing.T) crawlerSampleInfoHashesProducerFixture {
	t.Helper()
	events := &crawlerSampleInfoHashesProducerEventLog{}
	nodes := []*crawlerSampleInfoHashesProducerProbeNode{
		newCrawlerSampleInfoHashesProducerProbeNode("A", 1),
		newCrawlerSampleInfoHashesProducerProbeNode("B", 2),
		newCrawlerSampleInfoHashesProducerProbeNode("C", 3),
		newCrawlerSampleInfoHashesProducerProbeNode("D", 4),
	}
	selected := make([]ktable.Node, 0, len(nodes))
	for _, node := range nodes {
		selected = append(selected, node)
	}
	thirdGate := make(chan struct{})
	lane := &crawlerSampleInfoHashesProducerLane{
		input: make(chan ktable.Node, 2), entered: make(chan int, 8),
		gateAt: map[int]<-chan struct{}{3: thirdGate}, events: events,
	}
	table := &crawlerSampleInfoHashesProducerTable{
		nodes: selected, events: events,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := crawler{kTable: table, nodesForSampleInfoHashes: lane}
	done := make(chan struct{})
	go func() {
		c.getNodesForSampleInfoHashes(ctx)
		events.append("return")
		close(done)
	}()
	for want := 1; want <= 3; want++ {
		select {
		case got := <-lane.entered:
			if got != want {
				t.Fatalf("lane In call = %d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for lane In call %d", want)
		}
	}
	if len(lane.input) != 2 {
		t.Fatalf("queued prefix = %d, want 2", len(lane.input))
	}
	events.append("cancel")
	cancel()
	close(thirdGate)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sample-infohashes producer did not return after blocked-send cancellation")
	}

	deliveries := make([]crawlerSampleInfoHashesProducerDelivery, 0, 2)
	for index, want := range nodes[:2] {
		got := <-lane.input
		deliveries = append(deliveries, crawlerSampleInfoHashesProducerDelivery{
			Node: want.fixtureNode(), SameGoInterfaceHandle: got == want,
		})
		if got != want {
			t.Fatalf("delivery %d did not preserve the exact interface handle", index)
		}
	}
	calls := table.snapshotCalls()
	if len(calls) != 1 || calls[0].Limit != 60 {
		t.Fatalf("ordered-prefix table calls = %+v, want one limit-60 call", calls)
	}
	if lane.callCount() != 3 {
		t.Fatalf("ordered-prefix lane calls = %d, want 3", lane.callCount())
	}
	for _, node := range nodes {
		node.assertUntouched(t)
	}
	fixtureNodes := make([]crawlerSampleInfoHashesProducerNode, 0, len(nodes))
	accessorCalls := make([]crawlerSampleInfoHashesProducerAccessorCalls, 0, len(nodes))
	for _, node := range nodes {
		fixtureNodes = append(fixtureNodes, node.fixtureNode())
		accessorCalls = append(accessorCalls, node.fixtureAccessorCalls())
	}
	return crawlerSampleInfoHashesProducerFixture{
		ID:        "ordered_prefix_then_cancel_at_blocked_third_send",
		Subsystem: "dht_crawler_sample_infohashes_producer", Classification: "RUNTIME_EXACT",
		Oracle: crawlerSampleInfoHashesProducerRuntimeOracle("capacity_two_lane_with_third_In_gate"),
		Input: crawlerSampleInfoHashesProducerInput{
			Kind: "actual_getNodesForSampleInfoHashes", Nodes: fixtureNodes,
			LaneCapacity: 2, CancelAtLaneInCall: 3,
		},
		Expected: crawlerSampleInfoHashesProducerExpected{
			GetCalls: calls, LaneInCalls: 3, Deliveries: deliveries,
			Abandoned:     append([]crawlerSampleInfoHashesProducerNode{}, fixtureNodes[2:]...),
			AccessorCalls: accessorCalls, Events: events.snapshot(),
			RunReturned: true, ContextCancelled: true,
		},
	}
}

func crawlerSampleInfoHashesProducerRuntimeOracle(determinism string) crawlerSampleInfoHashesProducerOracle {
	return crawlerSampleInfoHashesProducerOracle{
		Composition: "actual_crawler_getNodesForSampleInfoHashes_with_scripted_table_and_manual_lane",
		Determinism: determinism, Table: "scripted_ktable_Table_GetNodesForSampleInfoHashes_override",
		Lane:  "capacity_controlled_BufferedConcurrentChannel_In_override",
		Clock: "production_unconditional_time_After_not_reached_by_runtime_row",
	}
}

func newCrawlerSampleInfoHashesProducerProbeNode(token string, value byte) *crawlerSampleInfoHashesProducerProbeNode {
	var id protocol.ID
	id[len(id)-1] = value
	return &crawlerSampleInfoHashesProducerProbeNode{
		token: token, id: id,
		addr: netip.MustParseAddrPort(fmt.Sprintf("192.0.2.%d:%d", value, 6000+int(value))),
	}
}

func assertCrawlerSampleInfoHashesProducerSourceShapes(t *testing.T) {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	producerSet, producer := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/sample_infohashes.go"), "getNodesForSampleInfoHashes")
	wantSet, want := crawlerFindNodeWorkerParseSourceFunc(t, `package dhtcrawler
func (c *crawler) getNodesForSampleInfoHashes(ctx context.Context) {
	for {
		peers := c.kTable.GetNodesForSampleInfoHashes(60)
		for _, p := range peers {
			select {
			case <-ctx.Done(): return
			case c.nodesForSampleInfoHashes.In() <- p: continue
			}
		}
		<-time.After(time.Second)
	}
}`)
	crawlerFindNodeWorkerAssertBody(t, producerSet, producer, wantSet, want,
		"getNodesForSampleInfoHashes")

	factorySet, factory := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/factory.go"), "New")
	values := make(map[string]ast.Expr)
	ast.Inspect(factory.Body, func(node ast.Node) bool {
		entry, ok := node.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		if key, ok := entry.Key.(*ast.Ident); ok {
			values[key.Name] = entry.Value
		}
		return true
	})
	crawlerPingWorkerAssertExpr(t, factorySet, values["nodesForSampleInfoHashes"],
		`concurrency.NewBufferedConcurrentChannel[ktable.Node](
			10*scalingFactor,
			10*scalingFactor,
		)`)

	crawlerSet, start := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/crawler.go"), "start")
	startText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, crawlerSet, start.Body))
	for _, required := range []string{
		"ctx, cancel := context.WithCancel(context.Background())",
		"defer cancel()", "go c.runSampleInfoHashes(ctx)",
		"go c.getNodesForSampleInfoHashes(ctx)", "<-c.stopped",
	} {
		if !bytes.Contains([]byte(startText), []byte(crawlerPingWorkerTokenText(required))) {
			t.Fatalf("crawler start missing %s", required)
		}
	}
	runSampleSet, runSample := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/sample_infohashes.go"), "runSampleInfoHashes")
	runSampleText := crawlerPingWorkerASTText(t, runSampleSet, runSample.Body)
	if !strings.Contains(runSampleText, "c.nodesForSampleInfoHashes.Run(ctx") {
		t.Fatal("sample-infohashes consumer no longer uses the producer's shared lane")
	}
	discoveredSet, runDiscovered := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/discovered_nodes.go"), "runDiscoveredNodes")
	wantDiscoveredSend := crawlerPingWorkerTokenText(
		"c.nodesForSampleInfoHashes.In() <- p")
	discoveredSendCount := 0
	ast.Inspect(runDiscovered.Body, func(node ast.Node) bool {
		send, ok := node.(*ast.SendStmt)
		if !ok {
			return true
		}
		if crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, discoveredSet, send)) ==
			wantDiscoveredSend {
			discoveredSendCount++
		}
		return true
	})
	if discoveredSendCount != 1 {
		t.Fatalf("runDiscoveredNodes sample-infohashes-lane sends = %d, want exactly 1",
			discoveredSendCount)
	}

	channelPath := filepath.Join(root, "internal/concurrency/buffered_concurrent_channel.go")
	channelSet, constructor := crawlerPingWorkerParseFunc(t, channelPath,
		"NewBufferedConcurrentChannel")
	wantChannelSet, wantConstructor := crawlerFindNodeWorkerParseSourceFunc(t, `package concurrency
func NewBufferedConcurrentChannel[T any](capacity int, concurrency int) BufferedConcurrentChannel[T] {
	return bufferedConcurrentChannel[T]{
		ch: make(chan T, capacity),
		sem: semaphore.NewWeighted(int64(concurrency)),
	}
}`)
	crawlerFindNodeWorkerAssertBody(t, channelSet, constructor,
		wantChannelSet, wantConstructor, "NewBufferedConcurrentChannel")

	channelSet, in := crawlerPingWorkerParseFunc(t, channelPath, "In")
	wantChannelSet, wantIn := crawlerFindNodeWorkerParseSourceFunc(t, `package concurrency
func (ch bufferedConcurrentChannel[T]) In() chan<- T { return ch.ch }`)
	crawlerFindNodeWorkerAssertBody(t, channelSet, in, wantChannelSet, wantIn,
		"BufferedConcurrentChannel.In")

	channelSet, run := crawlerPingWorkerParseFunc(t, channelPath, "Run")
	wantChannelSet, wantRun := crawlerFindNodeWorkerParseSourceFunc(t, `package concurrency
func (ch bufferedConcurrentChannel[T]) Run(ctx context.Context, f func(T)) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case next := <-ch.ch:
			if err := ch.sem.Acquire(ctx, 1); err != nil {
				return err
			}
			go func() {
				defer ch.sem.Release(1)
				f(next)
			}()
		}
	}
}`)
	crawlerFindNodeWorkerAssertBody(t, channelSet, run, wantChannelSet, wantRun,
		"BufferedConcurrentChannel.Run")

	assertCrawlerOldNodePingProducerMethodBody(t,
		filepath.Join(root, "internal/protocol/dht/ktable/table.go"),
		"*table", "GetNodesForSampleInfoHashes", `package ktable
func (t *table) GetNodesForSampleInfoHashes(n int) []Node {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return GetNodesForSampleInfoHashes{N: n}.execReturn(t)
}`)
	assertCrawlerOldNodePingProducerMethodBody(t,
		filepath.Join(root, "internal/protocol/dht/ktable/query.go"),
		"GetNodesForSampleInfoHashes", "execReturn", `package ktable
func (c GetNodesForSampleInfoHashes) execReturn(t *table) []Node {
	peers := make([]Node, 0, c.N)
	for _, p := range t.nodes.getCandidatesForSampleInfoHashes(c.N) {
		peers = append(peers, p)
		if len(peers) >= c.N {
			break
		}
	}
	return peers
}`)
	assertCrawlerOldNodePingProducerMethodBody(t,
		filepath.Join(root, "internal/protocol/dht/ktable/node.go"),
		"*nodeKeyspace", "getCandidatesForSampleInfoHashes", `package ktable
func (k *nodeKeyspace) getCandidatesForSampleInfoHashes(n int) []*node {
	var candidates []*node
	for _, it := range k.items {
		if !it.IsSampleInfoHashesCandidate() {
			continue
		}
		candidates = append(candidates, it)
		if len(candidates) == n {
			break
		}
	}
	return candidates
}`)
}

func crawlerSampleInfoHashesProducerSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	paths := []string{
		"internal/concurrency/buffered_concurrent_channel.go",
		"internal/dhtcrawler/config.go",
		"internal/dhtcrawler/crawler.go",
		"internal/dhtcrawler/discovered_nodes.go",
		"internal/dhtcrawler/factory.go",
		"internal/dhtcrawler/sample_infohashes.go",
		"internal/protocol/dht/ktable/node.go",
		"internal/protocol/dht/ktable/query.go",
		"internal/protocol/dht/ktable/table.go",
	}
	digests := make(map[string]string, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(contents)
		digests[path] = fmt.Sprintf("%x", digest)
	}
	return digests
}

func reconcileCrawlerSampleInfoHashesProducerFixtures(
	t *testing.T,
	fixtures []crawlerSampleInfoHashesProducerFixture,
) {
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
	if crawlerSampleInfoHashesProducerFixtureSHA256 != "" &&
		actualHash != crawlerSampleInfoHashesProducerFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash,
			crawlerSampleInfoHashesProducerFixtureSHA256)
	}
	path := filepath.Join(crawlerPingWorkerRoot(t),
		"testdata/parity/dht/dht_crawler_sample_infohashes_producer.jsonl")
	if *updateDHTCrawlerSampleInfoHashesProducerParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-sample-infohashes-producer-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler sample-infohashes-producer fixture is stale; rerun with -update-dht-crawler-sample-infohashes-producer-parity")
	}
}

var (
	_ concurrency.BufferedConcurrentChannel[ktable.Node] = (*crawlerSampleInfoHashesProducerLane)(nil)
	_ ktable.Table                                       = (*crawlerSampleInfoHashesProducerTable)(nil)
	_ ktable.Node                                        = (*crawlerSampleInfoHashesProducerProbeNode)(nil)
)
