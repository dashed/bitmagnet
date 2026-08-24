package dhtcrawler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
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

var updateDHTCrawlerOldNodePingProducerParity = flag.Bool(
	"update-dht-crawler-old-node-ping-producer-parity",
	false,
	"rewrite the Rust DHT crawler old-node ping-producer parity fixture",
)

const crawlerOldNodePingProducerFixtureSHA256 = "d300e4606f9811f402af6d835748d09dbc59434f733a28079ac0df5e2f99ae5a"

var crawlerOldNodePingProducerFixtureIDs = [...]string{
	"production_source_factory_and_lifecycle_contract",
	"already_cancelled_returns_before_initial_timer_and_query",
	"first_timer_ordered_prefix_then_cancel_at_blocked_third_send",
}

type crawlerOldNodePingProducerFixture struct {
	ID             string                             `json:"id"`
	Subsystem      string                             `json:"subsystem"`
	Classification string                             `json:"classification"`
	Oracle         crawlerOldNodePingProducerOracle   `json:"oracle"`
	Input          crawlerOldNodePingProducerInput    `json:"input"`
	Expected       crawlerOldNodePingProducerExpected `json:"expected"`
}

type crawlerOldNodePingProducerOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Table       string `json:"table"`
	Lane        string `json:"lane"`
	Clock       string `json:"clock"`
}

type crawlerOldNodePingProducerInput struct {
	Kind                      string                           `json:"kind"`
	ContextInitiallyCancelled bool                             `json:"contextInitiallyCancelled"`
	IntervalMS                int                              `json:"intervalMs"`
	OldPeerThresholdSeconds   int                              `json:"oldPeerThresholdSeconds"`
	Nodes                     []crawlerOldNodePingProducerNode `json:"nodes"`
	LaneCapacity              int                              `json:"laneCapacity"`
	CancelAtLaneInCall        int                              `json:"cancelAtLaneInCall"`
}

type crawlerOldNodePingProducerExpected struct {
	GetCalls         []crawlerOldNodePingProducerGetCall       `json:"getCalls"`
	LaneInCalls      int                                       `json:"laneInCalls"`
	Deliveries       []crawlerOldNodePingProducerDelivery      `json:"deliveries"`
	Abandoned        []crawlerOldNodePingProducerNode          `json:"abandoned"`
	AccessorCalls    []crawlerOldNodePingProducerAccessorCalls `json:"accessorCalls"`
	Events           []string                                  `json:"events"`
	RunReturned      bool                                      `json:"runReturned"`
	ContextCancelled bool                                      `json:"contextCancelled"`
	Source           *crawlerOldNodePingProducerSource         `json:"source,omitempty"`
}

type crawlerOldNodePingProducerGetCall struct {
	Limit                 int  `json:"limit"`
	CutoffWindowMatched   bool `json:"cutoffWindowMatched"`
	WaitedAtLeastInterval bool `json:"waitedAtLeastInterval"`
}

type crawlerOldNodePingProducerDelivery struct {
	Node                  crawlerOldNodePingProducerNode `json:"node"`
	SameGoInterfaceHandle bool                           `json:"sameGoInterfaceHandle"`
}

type crawlerOldNodePingProducerNode struct {
	Token string `json:"token"`
	ID    string `json:"id"`
	Addr  string `json:"addr"`
}

type crawlerOldNodePingProducerAccessorCalls struct {
	Token                     string `json:"token"`
	ID                        int    `json:"id"`
	Addr                      int    `json:"addr"`
	Time                      int    `json:"time"`
	Dropped                   int    `json:"dropped"`
	SampleInfoHashesCandidate int    `json:"sampleInfohashesCandidate"`
}

type crawlerOldNodePingProducerSource struct {
	InitialDelayBeforeFirstQuery   bool              `json:"initialDelayBeforeFirstQuery"`
	IntervalSeconds                int               `json:"intervalSeconds"`
	OldPeerThresholdSeconds        int               `json:"oldPeerThresholdSeconds"`
	Limit                          int               `json:"limit"`
	ZeroLimitIsUnbounded           bool              `json:"zeroLimitIsUnbounded"`
	StrictCutoff                   bool              `json:"strictCutoff"`
	ProductionSelectionOrder       string            `json:"productionSelectionOrder"`
	PreservesReturnedOrder         bool              `json:"preservesReturnedOrder"`
	PerNodeSendCancellationAware   bool              `json:"perNodeSendCancellationAware"`
	NoNodeProjectionOrRecheck      bool              `json:"noNodeProjectionOrRecheck"`
	FreshLeadingDelayPerLoop       bool              `json:"freshLeadingDelayPerLoop"`
	LeadingDelayCancellationAware  bool              `json:"leadingDelayCancellationAware"`
	EmptyTableCancellationOutcome  string            `json:"emptyTableCancellationOutcome"`
	ReadyTimerCancelOutcome        string            `json:"readyTimerCancelOutcome"`
	ReadySendCancelOutcome         string            `json:"readySendCancelOutcome"`
	ProducerDetached               bool              `json:"producerDetached"`
	ProducerJoined                 bool              `json:"producerJoined"`
	DefaultScalingFactor           int               `json:"defaultScalingFactor"`
	ProductionCapacity             int               `json:"productionCapacity"`
	ProductionConcurrency          int               `json:"productionConcurrency"`
	ConsumerDroppedGuard           bool              `json:"consumerDroppedGuard"`
	ConsumerRecentGuardStrictAfter bool              `json:"consumerRecentGuardStrictAfter"`
	ConsumerGuardAfterSemaphore    bool              `json:"consumerGuardAfterSemaphore"`
	CutoffClockRuntimeBracketed    bool              `json:"cutoffClockRuntimeBracketed"`
	PositiveTimerRuntimeObserved   bool              `json:"positiveTimerRuntimeObserved"`
	FactoryTimerRuntimeObserved    bool              `json:"factoryTimerRuntimeObserved"`
	SourceSHA256                   map[string]string `json:"sourceSha256"`
	Evidence                       string            `json:"evidence"`
}

type crawlerOldNodePingProducerEventLog struct {
	mutex  sync.Mutex
	events []string
}

func (l *crawlerOldNodePingProducerEventLog) append(event string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.events = append(l.events, event)
}

func (l *crawlerOldNodePingProducerEventLog) snapshot() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return append([]string{}, l.events...)
}

type crawlerOldNodePingProducerProbeNode struct {
	mutex      sync.Mutex
	token      string
	id         protocol.ID
	addr       netip.AddrPort
	idCalls    int
	addrCalls  int
	timeCalls  int
	dropCalls  int
	beP51Calls int
}

func (n *crawlerOldNodePingProducerProbeNode) ID() protocol.ID {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.idCalls++
	return n.id
}

func (n *crawlerOldNodePingProducerProbeNode) Addr() netip.AddrPort {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.addrCalls++
	return n.addr
}

func (n *crawlerOldNodePingProducerProbeNode) Time() time.Time {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.timeCalls++
	return time.Time{}
}

func (n *crawlerOldNodePingProducerProbeNode) Dropped() bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.dropCalls++
	return false
}

func (n *crawlerOldNodePingProducerProbeNode) IsSampleInfoHashesCandidate() bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.beP51Calls++
	return true
}

func (n *crawlerOldNodePingProducerProbeNode) fixtureNode() crawlerOldNodePingProducerNode {
	return crawlerOldNodePingProducerNode{Token: n.token, ID: n.id.String(), Addr: n.addr.String()}
}

func (n *crawlerOldNodePingProducerProbeNode) fixtureAccessorCalls() crawlerOldNodePingProducerAccessorCalls {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	return crawlerOldNodePingProducerAccessorCalls{
		Token: n.token, ID: n.idCalls, Addr: n.addrCalls, Time: n.timeCalls,
		Dropped: n.dropCalls, SampleInfoHashesCandidate: n.beP51Calls,
	}
}

func (n *crawlerOldNodePingProducerProbeNode) assertUntouched(t *testing.T) {
	t.Helper()
	calls := n.fixtureAccessorCalls()
	if calls.ID != 0 || calls.Addr != 0 || calls.Time != 0 || calls.Dropped != 0 ||
		calls.SampleInfoHashesCandidate != 0 {
		t.Fatalf("node %s accessor calls = %+v, want all zero", n.token, calls)
	}
}

type crawlerOldNodePingProducerTable struct {
	ktable.Table
	mutex     sync.Mutex
	nodes     []ktable.Node
	started   time.Time
	interval  time.Duration
	threshold time.Duration
	events    *crawlerOldNodePingProducerEventLog
	calls     []crawlerOldNodePingProducerGetCall
}

func (t *crawlerOldNodePingProducerTable) GetOldestNodes(
	cutoff time.Time,
	limit int,
) []ktable.Node {
	observed := time.Now()
	computedNow := cutoff.Add(t.threshold)
	call := crawlerOldNodePingProducerGetCall{
		Limit:                 limit,
		CutoffWindowMatched:   !computedNow.Before(t.started) && !computedNow.After(observed),
		WaitedAtLeastInterval: !computedNow.Before(t.started.Add(t.interval)),
	}
	t.mutex.Lock()
	t.calls = append(t.calls, call)
	t.mutex.Unlock()
	t.events.append("get_oldest_nodes")
	return append([]ktable.Node{}, t.nodes...)
}

func (t *crawlerOldNodePingProducerTable) snapshotCalls() []crawlerOldNodePingProducerGetCall {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return append([]crawlerOldNodePingProducerGetCall{}, t.calls...)
}

type crawlerOldNodePingProducerLane struct {
	input   chan ktable.Node
	entered chan int
	gateAt  map[int]<-chan struct{}
	events  *crawlerOldNodePingProducerEventLog
	mutex   sync.Mutex
	calls   int
}

func (l *crawlerOldNodePingProducerLane) In() chan<- ktable.Node {
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

func (*crawlerOldNodePingProducerLane) Run(context.Context, func(ktable.Node)) error {
	panic("old-node ping producer oracle must not run the consumer lane")
}

func (l *crawlerOldNodePingProducerLane) callCount() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.calls
}

func TestGenerateDHTCrawlerOldNodePingProducerParity(t *testing.T) {
	fixtures := []crawlerOldNodePingProducerFixture{
		crawlerOldNodePingProducerSourceFixture(t),
		runCrawlerOldNodePingProducerAlreadyCancelled(t),
		runCrawlerOldNodePingProducerOrderedPrefix(t),
	}
	if len(fixtures) != len(crawlerOldNodePingProducerFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerOldNodePingProducerFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerOldNodePingProducerFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerOldNodePingProducerFixtureIDs[index])
		}
		if fixture.Subsystem != "dht_crawler_old_node_ping_producer" {
			t.Fatalf("fixture %s subsystem = %q", fixture.ID, fixture.Subsystem)
		}
	}
	if fixtures[0].Classification != "SOURCE_ONLY" ||
		fixtures[1].Classification != "RUNTIME_EXACT" ||
		fixtures[2].Classification != "RUNTIME_EXACT" {
		t.Fatal("old-node ping producer fixture classifications drifted")
	}
	reconcileCrawlerOldNodePingProducerFixtures(t, fixtures)
}

func crawlerOldNodePingProducerSourceFixture(t *testing.T) crawlerOldNodePingProducerFixture {
	t.Helper()
	assertCrawlerOldNodePingProducerSourceShapes(t)
	scaling := int(NewDefaultConfig().ScalingFactor)
	if scaling != 10 {
		t.Fatalf("default scaling factor = %d, want 10", scaling)
	}
	return crawlerOldNodePingProducerFixture{
		ID:             "production_source_factory_and_lifecycle_contract",
		Subsystem:      "dht_crawler_old_node_ping_producer",
		Classification: "SOURCE_ONLY",
		Oracle: crawlerOldNodePingProducerOracle{
			Composition: "exact_production_source_factory_query_consumer_and_lifecycle_shapes",
			Determinism: "normalized_ast_and_whole_source_sha256",
			Table:       "production_ktable_Table_GetOldestNodes_strict_unbounded_query",
			Lane:        "production_buffered_concurrent_channel_shared_with_runPing",
			Clock:       "exact_source_time_After_then_time_Now_shapes",
		},
		Input: crawlerOldNodePingProducerInput{
			Kind: "source_contract", IntervalMS: 10_000, OldPeerThresholdSeconds: 900,
			Nodes: []crawlerOldNodePingProducerNode{}, LaneCapacity: scaling,
		},
		Expected: crawlerOldNodePingProducerExpected{
			GetCalls:      []crawlerOldNodePingProducerGetCall{},
			Deliveries:    []crawlerOldNodePingProducerDelivery{},
			Abandoned:     []crawlerOldNodePingProducerNode{},
			AccessorCalls: []crawlerOldNodePingProducerAccessorCalls{},
			Events:        []string{}, RunReturned: false,
			Source: &crawlerOldNodePingProducerSource{
				InitialDelayBeforeFirstQuery: true, IntervalSeconds: 10,
				OldPeerThresholdSeconds: 900, Limit: 0, ZeroLimitIsUnbounded: true,
				StrictCutoff:             true,
				ProductionSelectionOrder: "ascending_Time_with_unspecified_equal_time_order",
				PreservesReturnedOrder:   true, PerNodeSendCancellationAware: true,
				NoNodeProjectionOrRecheck: true, FreshLeadingDelayPerLoop: true,
				LeadingDelayCancellationAware: true,
				EmptyTableCancellationOutcome: "after_an_empty_query_the_loop_returns_to_a_fresh_cancellation_aware_leading_select",
				ReadyTimerCancelOutcome:       "go_select_chooses_nondeterministically_when_both_are_ready",
				ReadySendCancelOutcome:        "go_select_chooses_nondeterministically_when_both_are_ready",
				ProducerDetached:              true, ProducerJoined: false,
				DefaultScalingFactor: scaling, ProductionCapacity: scaling,
				ProductionConcurrency: scaling, ConsumerDroppedGuard: true,
				ConsumerRecentGuardStrictAfter: true, ConsumerGuardAfterSemaphore: true,
				CutoffClockRuntimeBracketed: true, PositiveTimerRuntimeObserved: true,
				FactoryTimerRuntimeObserved: false,
				SourceSHA256:                crawlerOldNodePingProducerSourceDigests(t),
				Evidence:                    "the actual method rows execute pre-cancel and a shortened positive timer; the factory ten-second timer, equal-ready select outcomes, production table order, and consumer callback scheduling remain source evidence",
			},
		},
	}
}

func runCrawlerOldNodePingProducerAlreadyCancelled(t *testing.T) crawlerOldNodePingProducerFixture {
	t.Helper()
	const interval = time.Minute
	const threshold = 15 * time.Minute
	events := &crawlerOldNodePingProducerEventLog{}
	lane := &crawlerOldNodePingProducerLane{
		input: make(chan ktable.Node), entered: make(chan int, 1),
		gateAt: map[int]<-chan struct{}{}, events: events,
	}
	started := time.Now()
	table := &crawlerOldNodePingProducerTable{
		started: started, interval: interval, threshold: threshold, events: events,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := crawler{
		kTable: table, nodesForPing: lane,
		getOldestNodesInterval: interval, oldPeerThreshold: threshold,
	}
	events.append("run_start")
	c.getOldNodes(ctx)
	events.append("return")

	if calls := table.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("pre-cancelled table calls = %+v, want none", calls)
	}
	if lane.callCount() != 0 || len(lane.input) != 0 {
		t.Fatalf("pre-cancelled lane calls/queued = %d/%d, want 0/0", lane.callCount(), len(lane.input))
	}
	return crawlerOldNodePingProducerFixture{
		ID:        "already_cancelled_returns_before_initial_timer_and_query",
		Subsystem: "dht_crawler_old_node_ping_producer", Classification: "RUNTIME_EXACT",
		Oracle: crawlerOldNodePingProducerRuntimeOracle(
			"pre_cancelled_context_with_positive_unobserved_timer"),
		Input: crawlerOldNodePingProducerInput{
			Kind: "actual_getOldNodes", ContextInitiallyCancelled: true,
			IntervalMS:              int(interval / time.Millisecond),
			OldPeerThresholdSeconds: int(threshold / time.Second),
			Nodes:                   []crawlerOldNodePingProducerNode{}, LaneCapacity: 0,
		},
		Expected: crawlerOldNodePingProducerExpected{
			GetCalls:      []crawlerOldNodePingProducerGetCall{},
			Deliveries:    []crawlerOldNodePingProducerDelivery{},
			Abandoned:     []crawlerOldNodePingProducerNode{},
			AccessorCalls: []crawlerOldNodePingProducerAccessorCalls{},
			Events:        events.snapshot(), RunReturned: true, ContextCancelled: true,
		},
	}
}

func runCrawlerOldNodePingProducerOrderedPrefix(t *testing.T) crawlerOldNodePingProducerFixture {
	t.Helper()
	const interval = 10 * time.Millisecond
	const threshold = 15 * time.Minute
	events := &crawlerOldNodePingProducerEventLog{}
	nodes := []*crawlerOldNodePingProducerProbeNode{
		newCrawlerOldNodePingProducerProbeNode("A", 1),
		newCrawlerOldNodePingProducerProbeNode("B", 2),
		newCrawlerOldNodePingProducerProbeNode("C", 3),
		newCrawlerOldNodePingProducerProbeNode("D", 4),
	}
	selected := make([]ktable.Node, 0, len(nodes))
	for _, node := range nodes {
		selected = append(selected, node)
	}
	thirdGate := make(chan struct{})
	var releaseOnce sync.Once
	releaseThird := func() { releaseOnce.Do(func() { close(thirdGate) }) }
	defer releaseThird()
	lane := &crawlerOldNodePingProducerLane{
		input: make(chan ktable.Node, 2), entered: make(chan int, 8),
		gateAt: map[int]<-chan struct{}{3: thirdGate}, events: events,
	}
	started := time.Now()
	table := &crawlerOldNodePingProducerTable{
		nodes: selected, started: started, interval: interval, threshold: threshold, events: events,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := crawler{
		kTable: table, nodesForPing: lane,
		getOldestNodesInterval: interval, oldPeerThreshold: threshold,
	}
	done := make(chan struct{})
	go func() {
		events.append("run_start")
		c.getOldNodes(ctx)
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
	releaseThird()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("old-node ping producer did not return after blocked-send cancellation")
	}

	deliveries := make([]crawlerOldNodePingProducerDelivery, 0, 2)
	for index, want := range nodes[:2] {
		got := <-lane.input
		deliveries = append(deliveries, crawlerOldNodePingProducerDelivery{
			Node: want.fixtureNode(), SameGoInterfaceHandle: got == want,
		})
		if got != want {
			t.Fatalf("delivery %d did not preserve the exact interface handle", index)
		}
	}
	calls := table.snapshotCalls()
	if len(calls) != 1 || calls[0].Limit != 0 || !calls[0].CutoffWindowMatched ||
		!calls[0].WaitedAtLeastInterval {
		t.Fatalf("ordered-prefix table calls = %+v, want one bracketed post-interval unlimited call", calls)
	}
	if lane.callCount() != 3 {
		t.Fatalf("ordered-prefix lane calls = %d, want 3", lane.callCount())
	}
	fixtureNodes := make([]crawlerOldNodePingProducerNode, 0, len(nodes))
	accessorCalls := make([]crawlerOldNodePingProducerAccessorCalls, 0, len(nodes))
	for _, node := range nodes {
		node.assertUntouched(t)
		fixtureNodes = append(fixtureNodes, node.fixtureNode())
		accessorCalls = append(accessorCalls, node.fixtureAccessorCalls())
	}
	return crawlerOldNodePingProducerFixture{
		ID:        "first_timer_ordered_prefix_then_cancel_at_blocked_third_send",
		Subsystem: "dht_crawler_old_node_ping_producer", Classification: "RUNTIME_EXACT",
		Oracle: crawlerOldNodePingProducerRuntimeOracle(
			"shortened_positive_timer_and_capacity_two_lane_with_third_In_gate"),
		Input: crawlerOldNodePingProducerInput{
			Kind: "actual_getOldNodes", IntervalMS: int(interval / time.Millisecond),
			OldPeerThresholdSeconds: int(threshold / time.Second), Nodes: fixtureNodes,
			LaneCapacity: 2, CancelAtLaneInCall: 3,
		},
		Expected: crawlerOldNodePingProducerExpected{
			GetCalls: calls, LaneInCalls: 3, Deliveries: deliveries,
			Abandoned:     append([]crawlerOldNodePingProducerNode{}, fixtureNodes[2:]...),
			AccessorCalls: accessorCalls, Events: events.snapshot(),
			RunReturned: true, ContextCancelled: true,
		},
	}
}

func crawlerOldNodePingProducerRuntimeOracle(determinism string) crawlerOldNodePingProducerOracle {
	return crawlerOldNodePingProducerOracle{
		Composition: "actual_crawler_getOldNodes_with_scripted_table_and_manual_lane",
		Determinism: determinism, Table: "scripted_ktable_Table_GetOldestNodes_override",
		Lane:  "capacity_controlled_BufferedConcurrentChannel_In_override",
		Clock: "production_time_After_and_time_Now_with_runtime_bracketed_cutoff",
	}
}

func newCrawlerOldNodePingProducerProbeNode(
	token string,
	value byte,
) *crawlerOldNodePingProducerProbeNode {
	var id protocol.ID
	id[len(id)-1] = value
	return &crawlerOldNodePingProducerProbeNode{
		token: token, id: id,
		addr: netip.MustParseAddrPort(fmt.Sprintf("192.0.2.%d:%d", value, 7000+int(value))),
	}
}

func assertCrawlerOldNodePingProducerSourceShapes(t *testing.T) {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	producerSet, producer := crawlerPingWorkerParseFunc(
		t, filepath.Join(root, "internal/dhtcrawler/ping.go"), "getOldNodes")
	wantSet, want := crawlerFindNodeWorkerParseSourceFunc(t, `package dhtcrawler
func (c *crawler) getOldNodes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): return
		case <-time.After(c.getOldestNodesInterval):
			for _, p := range c.kTable.GetOldestNodes(time.Now().Add(-c.oldPeerThreshold), 0) {
				select {
				case <-ctx.Done(): return
				case c.nodesForPing.In() <- p: continue
				}
			}
		}
	}
}`)
	crawlerFindNodeWorkerAssertBody(t, producerSet, producer, wantSet, want, "getOldNodes")

	assertCrawlerPingWorkerSourceShapes(t)
	factorySet, factory := crawlerPingWorkerParseFunc(
		t, filepath.Join(root, "internal/dhtcrawler/factory.go"), "New")
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
	crawlerPingWorkerAssertExpr(t, factorySet, values["getOldestNodesInterval"], "time.Second * 10")
	crawlerPingWorkerAssertExpr(t, factorySet, values["oldPeerThreshold"], "time.Minute * 15")
	crawlerPingWorkerAssertExpr(t, factorySet, values["nodesForPing"],
		"concurrency.NewBufferedConcurrentChannel[ktable.Node](scalingFactor, scalingFactor)")

	crawlerSet, start := crawlerPingWorkerParseFunc(
		t, filepath.Join(root, "internal/dhtcrawler/crawler.go"), "start")
	startText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, crawlerSet, start.Body))
	for _, required := range []string{
		"ctx, cancel := context.WithCancel(context.Background())", "defer cancel()",
		"go c.runPing(ctx)", "go c.getOldNodes(ctx)", "<-c.stopped",
	} {
		if !strings.Contains(startText, crawlerPingWorkerTokenText(required)) {
			t.Fatalf("crawler start missing %s", required)
		}
	}

	channelSet, in := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/concurrency/buffered_concurrent_channel.go"), "In")
	wantChannelSet, wantIn := crawlerFindNodeWorkerParseSourceFunc(t, `package concurrency
func (ch bufferedConcurrentChannel[T]) In() chan<- T { return ch.ch }`)
	crawlerFindNodeWorkerAssertBody(t, channelSet, in, wantChannelSet, wantIn,
		"BufferedConcurrentChannel.In")

	assertCrawlerOldNodePingProducerMethodBody(t,
		filepath.Join(root, "internal/protocol/dht/ktable/table.go"), "*table", "GetOldestNodes",
		`package ktable
func (t *table) GetOldestNodes(cutoff time.Time, n int) []Node {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return GetOldestPeers{
		Cutoff: cutoff,
		N:      n,
	}.execReturn(t)
}`)
	assertCrawlerOldNodePingProducerMethodBody(t,
		filepath.Join(root, "internal/protocol/dht/ktable/node.go"), "*nodeKeyspace", "getLastRespondedBefore",
		`package ktable
func (k *nodeKeyspace) getLastRespondedBefore(t time.Time) []Node {
	var peers []Node

	for _, it := range k.items {
		if it.lastRespondedAt.Before(t) {
			peers = append(peers, it)
		}
	}

	return peers
}`)
	assertCrawlerOldNodePingProducerMethodBody(t,
		filepath.Join(root, "internal/protocol/dht/ktable/query.go"), "GetOldestPeers", "execReturn",
		`package ktable
func (c GetOldestPeers) execReturn(t *table) []Node {
	peers := t.nodes.getLastRespondedBefore(c.Cutoff)
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].Time().Before(peers[j].Time())
	})

	if c.N > 0 && len(peers) > c.N {
		peers = peers[:c.N]
	}

	return peers
}`)
}

func assertCrawlerOldNodePingProducerMethodBody(
	t *testing.T,
	path string,
	receiver string,
	name string,
	wantSource string,
) {
	t.Helper()
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != name || function.Recv == nil || len(function.Recv.List) != 1 {
			continue
		}
		if crawlerPingWorkerASTText(t, set, function.Recv.List[0].Type) != receiver {
			continue
		}
		wantSet, want := crawlerFindNodeWorkerParseSourceFunc(t, wantSource)
		crawlerFindNodeWorkerAssertBody(t, set, function, wantSet, want,
			fmt.Sprintf("%s.%s", receiver, name))
		return
	}
	t.Fatalf("method %s.%s not found in %s", receiver, name, path)
}

func crawlerOldNodePingProducerSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	paths := []string{
		"internal/concurrency/buffered_concurrent_channel.go",
		"internal/dhtcrawler/config.go",
		"internal/dhtcrawler/crawler.go",
		"internal/dhtcrawler/factory.go",
		"internal/dhtcrawler/ping.go",
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

func reconcileCrawlerOldNodePingProducerFixtures(
	t *testing.T,
	fixtures []crawlerOldNodePingProducerFixture,
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
	if crawlerOldNodePingProducerFixtureSHA256 != "" &&
		actualHash != crawlerOldNodePingProducerFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash,
			crawlerOldNodePingProducerFixtureSHA256)
	}
	path := filepath.Join(crawlerPingWorkerRoot(t),
		"testdata/parity/dht/dht_crawler_old_node_ping_producer.jsonl")
	if *updateDHTCrawlerOldNodePingProducerParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-old-node-ping-producer-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler old-node ping-producer fixture is stale; rerun with -update-dht-crawler-old-node-ping-producer-parity")
	}
}

var _ concurrency.BufferedConcurrentChannel[ktable.Node] = (*crawlerOldNodePingProducerLane)(nil)
var _ ktable.Table = (*crawlerOldNodePingProducerTable)(nil)
var _ ktable.Node = (*crawlerOldNodePingProducerProbeNode)(nil)
