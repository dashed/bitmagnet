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

var updateDHTCrawlerFindNodeProducerParity = flag.Bool(
	"update-dht-crawler-find-node-producer-parity",
	false,
	"rewrite the Rust DHT crawler find-node-producer parity fixture",
)

const crawlerFindNodeProducerFixtureSHA256 = "06e2ac78f73418038c946fdc5f3562654e130623fcf88e907c1c4e07112505cc"

var crawlerFindNodeProducerFixtureIDs = [...]string{
	"production_source_factory_and_lifecycle_contract",
	"already_cancelled_still_queries_before_first_send",
	"ordered_prefix_then_cancel_at_blocked_third_send",
}

type crawlerFindNodeProducerFixture struct {
	ID             string                          `json:"id"`
	Subsystem      string                          `json:"subsystem"`
	Classification string                          `json:"classification"`
	Oracle         crawlerFindNodeProducerOracle   `json:"oracle"`
	Input          crawlerFindNodeProducerInput    `json:"input"`
	Expected       crawlerFindNodeProducerExpected `json:"expected"`
}

type crawlerFindNodeProducerOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Table       string `json:"table"`
	Lane        string `json:"lane"`
	Clock       string `json:"clock"`
}

type crawlerFindNodeProducerInput struct {
	Kind                      string                        `json:"kind"`
	ContextInitiallyCancelled bool                          `json:"contextInitiallyCancelled"`
	Nodes                     []crawlerFindNodeProducerNode `json:"nodes"`
	LaneCapacity              int                           `json:"laneCapacity"`
	CancelAtLaneInCall        int                           `json:"cancelAtLaneInCall"`
}

type crawlerFindNodeProducerExpected struct {
	GetCalls         []crawlerFindNodeProducerGetCall       `json:"getCalls"`
	LaneInCalls      int                                    `json:"laneInCalls"`
	Deliveries       []crawlerFindNodeProducerDelivery      `json:"deliveries"`
	Abandoned        []crawlerFindNodeProducerNode          `json:"abandoned"`
	AccessorCalls    []crawlerFindNodeProducerAccessorCalls `json:"accessorCalls"`
	Events           []string                               `json:"events"`
	RunReturned      bool                                   `json:"runReturned"`
	ContextCancelled bool                                   `json:"contextCancelled"`
	Source           *crawlerFindNodeProducerSource         `json:"source,omitempty"`
}

type crawlerFindNodeProducerGetCall struct {
	Limit               int  `json:"limit"`
	CutoffWindowMatched bool `json:"cutoffWindowMatched"`
}

type crawlerFindNodeProducerDelivery struct {
	Node                  crawlerFindNodeProducerNode `json:"node"`
	SameGoInterfaceHandle bool                        `json:"sameGoInterfaceHandle"`
}

type crawlerFindNodeProducerNode struct {
	Token string `json:"token"`
	ID    string `json:"id"`
	Addr  string `json:"addr"`
}

type crawlerFindNodeProducerAccessorCalls struct {
	Token                     string `json:"token"`
	ID                        int    `json:"id"`
	Addr                      int    `json:"addr"`
	Time                      int    `json:"time"`
	Dropped                   int    `json:"dropped"`
	SampleInfoHashesCandidate int    `json:"sampleInfohashesCandidate"`
}

type crawlerFindNodeProducerSource struct {
	ImmediateFirstQuery             bool              `json:"immediateFirstQuery"`
	CutoffSeconds                   int               `json:"cutoffSeconds"`
	Limit                           int               `json:"limit"`
	PreservesReturnedOrder          bool              `json:"preservesReturnedOrder"`
	PerNodeSendCancellationAware    bool              `json:"perNodeSendCancellationAware"`
	NoNodeProjectionOrRecheck       bool              `json:"noNodeProjectionOrRecheck"`
	PostBatchDelayMS                int               `json:"postBatchDelayMs"`
	PostBatchSleepCancellationAware bool              `json:"postBatchSleepCancellationAware"`
	EmptyTableCancellationOutcome   string            `json:"emptyTableCancellationOutcome"`
	ReadySendCancelOutcome          string            `json:"readySendCancelOutcome"`
	ProducerDetached                bool              `json:"producerDetached"`
	ProducerJoined                  bool              `json:"producerJoined"`
	ProductionCapacity              int               `json:"productionCapacity"`
	ProductionConcurrency           int               `json:"productionConcurrency"`
	CutoffClockRuntimeBracketed     bool              `json:"cutoffClockRuntimeBracketed"`
	PostBatchDelayRuntimeObserved   bool              `json:"postBatchDelayRuntimeObserved"`
	EmptyTableRuntimeObserved       bool              `json:"emptyTableRuntimeObserved"`
	RuntimeRowsReturnBeforeSleep    bool              `json:"runtimeRowsReturnBeforeSleep"`
	SourceSHA256                    map[string]string `json:"sourceSha256"`
	Evidence                        string            `json:"evidence"`
}

type crawlerFindNodeProducerEventLog struct {
	mutex  sync.Mutex
	events []string
}

func (l *crawlerFindNodeProducerEventLog) append(event string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.events = append(l.events, event)
}

func (l *crawlerFindNodeProducerEventLog) snapshot() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return append([]string{}, l.events...)
}

type crawlerFindNodeProducerProbeNode struct {
	token      string
	id         protocol.ID
	addr       netip.AddrPort
	idCalls    int
	addrCalls  int
	timeCalls  int
	dropCalls  int
	beP51Calls int
}

func (n *crawlerFindNodeProducerProbeNode) ID() protocol.ID {
	n.idCalls++
	return n.id
}

func (n *crawlerFindNodeProducerProbeNode) Addr() netip.AddrPort {
	n.addrCalls++
	return n.addr
}

func (n *crawlerFindNodeProducerProbeNode) Time() time.Time {
	n.timeCalls++
	return time.Time{}
}

func (n *crawlerFindNodeProducerProbeNode) Dropped() bool {
	n.dropCalls++
	return false
}

func (n *crawlerFindNodeProducerProbeNode) IsSampleInfoHashesCandidate() bool {
	n.beP51Calls++
	return true
}

func (n *crawlerFindNodeProducerProbeNode) assertUntouched(t *testing.T) {
	t.Helper()
	if n.idCalls != 0 || n.addrCalls != 0 || n.timeCalls != 0 || n.dropCalls != 0 || n.beP51Calls != 0 {
		t.Fatalf("node %s accessor calls = id:%d addr:%d time:%d dropped:%d bep51:%d, want all zero",
			n.token, n.idCalls, n.addrCalls, n.timeCalls, n.dropCalls, n.beP51Calls)
	}
}

func (n *crawlerFindNodeProducerProbeNode) fixtureNode() crawlerFindNodeProducerNode {
	return crawlerFindNodeProducerNode{Token: n.token, ID: n.id.String(), Addr: n.addr.String()}
}

func (n *crawlerFindNodeProducerProbeNode) fixtureAccessorCalls() crawlerFindNodeProducerAccessorCalls {
	return crawlerFindNodeProducerAccessorCalls{
		Token: n.token, ID: n.idCalls, Addr: n.addrCalls, Time: n.timeCalls,
		Dropped: n.dropCalls, SampleInfoHashesCandidate: n.beP51Calls,
	}
}

type crawlerFindNodeProducerTable struct {
	ktable.Table
	mutex   sync.Mutex
	nodes   []ktable.Node
	started time.Time
	events  *crawlerFindNodeProducerEventLog
	calls   []crawlerFindNodeProducerGetCall
}

func (t *crawlerFindNodeProducerTable) GetOldestNodes(cutoff time.Time, limit int) []ktable.Node {
	observed := time.Now()
	computedNow := cutoff.Add(5 * time.Second)
	call := crawlerFindNodeProducerGetCall{
		Limit:               limit,
		CutoffWindowMatched: !computedNow.Before(t.started) && !computedNow.After(observed),
	}
	t.mutex.Lock()
	t.calls = append(t.calls, call)
	t.mutex.Unlock()
	t.events.append("get_oldest_nodes")
	return append([]ktable.Node{}, t.nodes...)
}

func (t *crawlerFindNodeProducerTable) snapshotCalls() []crawlerFindNodeProducerGetCall {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return append([]crawlerFindNodeProducerGetCall{}, t.calls...)
}

type crawlerFindNodeProducerLane struct {
	input   chan ktable.Node
	entered chan int
	gateAt  map[int]<-chan struct{}
	events  *crawlerFindNodeProducerEventLog
	mutex   sync.Mutex
	calls   int
}

func (l *crawlerFindNodeProducerLane) In() chan<- ktable.Node {
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

func (*crawlerFindNodeProducerLane) Run(context.Context, func(ktable.Node)) error {
	panic("find-node producer oracle must not run the consumer lane")
}

func (l *crawlerFindNodeProducerLane) callCount() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.calls
}

func TestGenerateDHTCrawlerFindNodeProducerParity(t *testing.T) {
	fixtures := []crawlerFindNodeProducerFixture{
		crawlerFindNodeProducerSourceFixture(t),
		runCrawlerFindNodeProducerAlreadyCancelled(t),
		runCrawlerFindNodeProducerOrderedPrefix(t),
	}
	if len(fixtures) != len(crawlerFindNodeProducerFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerFindNodeProducerFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerFindNodeProducerFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerFindNodeProducerFixtureIDs[index])
		}
		if fixture.Subsystem != "dht_crawler_find_node_producer" {
			t.Fatalf("fixture %s subsystem = %q", fixture.ID, fixture.Subsystem)
		}
	}
	if fixtures[0].Classification != "SOURCE_ONLY" ||
		fixtures[1].Classification != "RUNTIME_EXACT" ||
		fixtures[2].Classification != "RUNTIME_EXACT" {
		t.Fatal("find-node producer fixture classifications drifted")
	}
	reconcileCrawlerFindNodeProducerFixtures(t, fixtures)
}

func crawlerFindNodeProducerSourceFixture(t *testing.T) crawlerFindNodeProducerFixture {
	t.Helper()
	assertCrawlerFindNodeProducerSourceShapes(t)
	scaling := int(NewDefaultConfig().ScalingFactor)
	if scaling != 10 {
		t.Fatalf("default scaling factor = %d, want 10", scaling)
	}
	return crawlerFindNodeProducerFixture{
		ID:             "production_source_factory_and_lifecycle_contract",
		Subsystem:      "dht_crawler_find_node_producer",
		Classification: "SOURCE_ONLY",
		Oracle: crawlerFindNodeProducerOracle{
			Composition: "exact_production_source_factory_and_lifecycle_shapes",
			Determinism: "normalized_ast_and_whole_source_sha256",
			Table:       "production_ktable_Table_GetOldestNodes_interface",
			Lane:        "production_buffered_concurrent_channel",
			Clock:       "exact_source_time_Now_and_time_After_shapes",
		},
		Input: crawlerFindNodeProducerInput{
			Kind: "source_contract", Nodes: []crawlerFindNodeProducerNode{},
		},
		Expected: crawlerFindNodeProducerExpected{
			GetCalls: []crawlerFindNodeProducerGetCall{}, Deliveries: []crawlerFindNodeProducerDelivery{},
			Abandoned:     []crawlerFindNodeProducerNode{},
			AccessorCalls: []crawlerFindNodeProducerAccessorCalls{},
			Events:        []string{}, RunReturned: false,
			Source: &crawlerFindNodeProducerSource{
				ImmediateFirstQuery: true, CutoffSeconds: 5, Limit: 10,
				PreservesReturnedOrder: true, PerNodeSendCancellationAware: true,
				NoNodeProjectionOrRecheck: true, PostBatchDelayMS: 1000,
				PostBatchSleepCancellationAware: false,
				EmptyTableCancellationOutcome:   "while_every_query_remains_empty_queries_then_unconditionally_sleeps_one_second_forever",
				ReadySendCancelOutcome:          "go_select_chooses_nondeterministically_when_both_are_ready",
				ProducerDetached:                true, ProducerJoined: false,
				ProductionCapacity: 10 * scaling, ProductionConcurrency: 10 * scaling,
				CutoffClockRuntimeBracketed:   true,
				PostBatchDelayRuntimeObserved: false, EmptyTableRuntimeObserved: false,
				RuntimeRowsReturnBeforeSleep: true,
				SourceSHA256:                 crawlerFindNodeProducerSourceDigests(t),
				Evidence:                     "the cutoff clock is runtime-bracketed; post-batch timer timing and empty-table cancellation are source-only because runtime rows return from the actual method before its real sleep",
			},
		},
	}
}

func runCrawlerFindNodeProducerAlreadyCancelled(t *testing.T) crawlerFindNodeProducerFixture {
	t.Helper()
	events := &crawlerFindNodeProducerEventLog{}
	node := newCrawlerFindNodeProducerProbeNode("A", 1)
	lane := &crawlerFindNodeProducerLane{
		input: make(chan ktable.Node), entered: make(chan int, 4),
		gateAt: map[int]<-chan struct{}{}, events: events,
	}
	table := &crawlerFindNodeProducerTable{
		nodes: []ktable.Node{node}, started: time.Now(), events: events,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := crawler{kTable: table, nodesForFindNode: lane}
	c.getNodesForFindNode(ctx)
	events.append("return")

	calls := table.snapshotCalls()
	if len(calls) != 1 || calls[0].Limit != 10 || !calls[0].CutoffWindowMatched {
		t.Fatalf("already-cancelled table calls = %+v, want one bracketed limit-10 call", calls)
	}
	if lane.callCount() != 1 || len(lane.input) != 0 {
		t.Fatalf("already-cancelled lane calls/queued = %d/%d, want 1/0", lane.callCount(), len(lane.input))
	}
	node.assertUntouched(t)
	return crawlerFindNodeProducerFixture{
		ID:        "already_cancelled_still_queries_before_first_send",
		Subsystem: "dht_crawler_find_node_producer", Classification: "RUNTIME_EXACT",
		Oracle: crawlerFindNodeProducerRuntimeOracle("pre_cancelled_context_and_unbuffered_lane"),
		Input: crawlerFindNodeProducerInput{
			Kind: "actual_getNodesForFindNode", ContextInitiallyCancelled: true,
			Nodes: []crawlerFindNodeProducerNode{node.fixtureNode()}, LaneCapacity: 0,
		},
		Expected: crawlerFindNodeProducerExpected{
			GetCalls: calls, LaneInCalls: 1,
			Deliveries:    []crawlerFindNodeProducerDelivery{},
			Abandoned:     []crawlerFindNodeProducerNode{node.fixtureNode()},
			AccessorCalls: []crawlerFindNodeProducerAccessorCalls{node.fixtureAccessorCalls()},
			Events:        events.snapshot(), RunReturned: true, ContextCancelled: true,
		},
	}
}

func runCrawlerFindNodeProducerOrderedPrefix(t *testing.T) crawlerFindNodeProducerFixture {
	t.Helper()
	events := &crawlerFindNodeProducerEventLog{}
	nodes := []*crawlerFindNodeProducerProbeNode{
		newCrawlerFindNodeProducerProbeNode("A", 1),
		newCrawlerFindNodeProducerProbeNode("B", 2),
		newCrawlerFindNodeProducerProbeNode("C", 3),
		newCrawlerFindNodeProducerProbeNode("D", 4),
	}
	selected := make([]ktable.Node, 0, len(nodes))
	for _, node := range nodes {
		selected = append(selected, node)
	}
	thirdGate := make(chan struct{})
	lane := &crawlerFindNodeProducerLane{
		input: make(chan ktable.Node, 2), entered: make(chan int, 8),
		gateAt: map[int]<-chan struct{}{3: thirdGate}, events: events,
	}
	table := &crawlerFindNodeProducerTable{
		nodes: selected, started: time.Now(), events: events,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := crawler{kTable: table, nodesForFindNode: lane}
	done := make(chan struct{})
	go func() {
		c.getNodesForFindNode(ctx)
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
		t.Fatal("find-node producer did not return after blocked-send cancellation")
	}

	deliveries := make([]crawlerFindNodeProducerDelivery, 0, 2)
	for index, want := range nodes[:2] {
		got := <-lane.input
		deliveries = append(deliveries, crawlerFindNodeProducerDelivery{
			Node: want.fixtureNode(), SameGoInterfaceHandle: got == want,
		})
		if got != want {
			t.Fatalf("delivery %d did not preserve the exact interface handle", index)
		}
	}
	calls := table.snapshotCalls()
	if len(calls) != 1 || calls[0].Limit != 10 || !calls[0].CutoffWindowMatched {
		t.Fatalf("ordered-prefix table calls = %+v, want one bracketed limit-10 call", calls)
	}
	if lane.callCount() != 3 {
		t.Fatalf("ordered-prefix lane calls = %d, want 3", lane.callCount())
	}
	for _, node := range nodes {
		node.assertUntouched(t)
	}
	fixtureNodes := make([]crawlerFindNodeProducerNode, 0, len(nodes))
	accessorCalls := make([]crawlerFindNodeProducerAccessorCalls, 0, len(nodes))
	for _, node := range nodes {
		fixtureNodes = append(fixtureNodes, node.fixtureNode())
		accessorCalls = append(accessorCalls, node.fixtureAccessorCalls())
	}
	return crawlerFindNodeProducerFixture{
		ID:        "ordered_prefix_then_cancel_at_blocked_third_send",
		Subsystem: "dht_crawler_find_node_producer", Classification: "RUNTIME_EXACT",
		Oracle: crawlerFindNodeProducerRuntimeOracle("capacity_two_lane_with_third_In_gate"),
		Input: crawlerFindNodeProducerInput{
			Kind: "actual_getNodesForFindNode", Nodes: fixtureNodes,
			LaneCapacity: 2, CancelAtLaneInCall: 3,
		},
		Expected: crawlerFindNodeProducerExpected{
			GetCalls: calls, LaneInCalls: 3, Deliveries: deliveries,
			Abandoned:     append([]crawlerFindNodeProducerNode{}, fixtureNodes[2:]...),
			AccessorCalls: accessorCalls, Events: events.snapshot(),
			RunReturned: true, ContextCancelled: true,
		},
	}
}

func crawlerFindNodeProducerRuntimeOracle(determinism string) crawlerFindNodeProducerOracle {
	return crawlerFindNodeProducerOracle{
		Composition: "actual_crawler_getNodesForFindNode_with_scripted_table_and_manual_lane",
		Determinism: determinism, Table: "scripted_ktable_Table_GetOldestNodes_override",
		Lane:  "capacity_controlled_BufferedConcurrentChannel_In_override",
		Clock: "production_time_Now_cutoff_runtime_bracketed_without_reaching_time_After",
	}
}

func newCrawlerFindNodeProducerProbeNode(token string, value byte) *crawlerFindNodeProducerProbeNode {
	var id protocol.ID
	id[len(id)-1] = value
	return &crawlerFindNodeProducerProbeNode{
		token: token, id: id,
		addr: netip.MustParseAddrPort(fmt.Sprintf("192.0.2.%d:%d", value, 6000+int(value))),
	}
}

func assertCrawlerFindNodeProducerSourceShapes(t *testing.T) {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	producerSet, producer := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/find_node.go"), "getNodesForFindNode")
	wantSet, want := crawlerFindNodeWorkerParseSourceFunc(t, `package dhtcrawler
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
	crawlerFindNodeWorkerAssertBody(t, producerSet, producer, wantSet, want,
		"getNodesForFindNode")

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
	crawlerPingWorkerAssertExpr(t, factorySet, values["nodesForFindNode"],
		"concurrency.NewBufferedConcurrentChannel[ktable.Node](10*scalingFactor, 10*scalingFactor)")

	crawlerSet, start := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/crawler.go"), "start")
	startText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, crawlerSet, start.Body))
	for _, required := range []string{
		"ctx, cancel := context.WithCancel(context.Background())",
		"defer cancel()", "go c.getNodesForFindNode(ctx)", "<-c.stopped",
	} {
		if !bytes.Contains([]byte(startText), []byte(crawlerPingWorkerTokenText(required))) {
			t.Fatalf("crawler start missing %s", required)
		}
	}
	runFindSet, runFind := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/find_node.go"), "runFindNode")
	runFindText := crawlerPingWorkerASTText(t, runFindSet, runFind.Body)
	if !strings.Contains(runFindText, "c.nodesForFindNode.Run(ctx") {
		t.Fatal("find-node consumer no longer uses the producer's shared lane")
	}

	channelSet, in := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/concurrency/buffered_concurrent_channel.go"), "In")
	wantChannelSet, wantIn := crawlerFindNodeWorkerParseSourceFunc(t, `package concurrency
func (ch bufferedConcurrentChannel[T]) In() chan<- T { return ch.ch }`)
	crawlerFindNodeWorkerAssertBody(t, channelSet, in, wantChannelSet, wantIn,
		"BufferedConcurrentChannel.In")
}

func crawlerFindNodeProducerSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	paths := []string{
		"internal/concurrency/buffered_concurrent_channel.go",
		"internal/dhtcrawler/config.go",
		"internal/dhtcrawler/crawler.go",
		"internal/dhtcrawler/factory.go",
		"internal/dhtcrawler/find_node.go",
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

func reconcileCrawlerFindNodeProducerFixtures(
	t *testing.T,
	fixtures []crawlerFindNodeProducerFixture,
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
	if crawlerFindNodeProducerFixtureSHA256 != "" &&
		actualHash != crawlerFindNodeProducerFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash,
			crawlerFindNodeProducerFixtureSHA256)
	}
	path := filepath.Join(crawlerPingWorkerRoot(t),
		"testdata/parity/dht/dht_crawler_find_node_producer.jsonl")
	if *updateDHTCrawlerFindNodeProducerParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-find-node-producer-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler find-node-producer fixture is stale; rerun with -update-dht-crawler-find-node-producer-parity")
	}
}

var (
	_ concurrency.BufferedConcurrentChannel[ktable.Node] = (*crawlerFindNodeProducerLane)(nil)
	_ ktable.Table                                       = (*crawlerFindNodeProducerTable)(nil)
	_ ktable.Node                                        = (*crawlerFindNodeProducerProbeNode)(nil)
)
