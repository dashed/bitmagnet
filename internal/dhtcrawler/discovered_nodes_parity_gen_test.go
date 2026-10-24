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
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable/btree"
)

var updateDHTCrawlerDiscoveredNodesParity = flag.Bool(
	"update-dht-crawler-discovered-nodes-parity",
	false,
	"rewrite the Rust DHT crawler discovered-nodes parity fixture",
)

const crawlerDiscoveredNodesFixtureSHA256 = "ae6d867378a227284aa0cd93e9120d70afbec1c5e3b19a9f64e09edace4190e0"

var crawlerDiscoveredNodesFixtureIDs = [...]string{
	"production_factory_defaults_and_source_lifecycle",
	"size_flush_order_and_output_backpressure",
	"first_ip_wins_known_filter_and_only_ping_ready",
	"cross_batch_dedupe_resets_and_all_known_continues",
	"only_find_node_ready",
	"only_sample_infohashes_ready",
	"all_ready_routes_each_node_exactly_once",
	"all_full_then_ping_drain_routes",
	"cancel_after_one_delivery_abandons_blocked_suffix",
	"blocked_route_cancellation_exits_without_delivery",
}

type crawlerDiscoveredNodesFixture struct {
	ID        string                         `json:"id"`
	Subsystem string                         `json:"subsystem"`
	Oracle    crawlerDiscoveredNodesOracle   `json:"oracle"`
	Input     crawlerDiscoveredNodesInput    `json:"input"`
	Expected  crawlerDiscoveredNodesExpected `json:"expected"`
}

type crawlerDiscoveredNodesOracle struct {
	Composition       string `json:"composition"`
	Determinism       string `json:"determinism"`
	Ingress           string `json:"ingress"`
	Routes            string `json:"routes"`
	AddressProjection string `json:"addressProjection"`
}

type crawlerDiscoveredNodesInput struct {
	Kind          string                              `json:"kind"`
	ScalingFactor uint                                `json:"scalingFactor,omitempty"`
	Values        []int                               `json:"values,omitempty"`
	Batches       [][]crawlerDiscoveredNodesInputNode `json:"batches,omitempty"`
	TableSetup    []crawlerDiscoveredNodesTableSetup  `json:"tableSetup,omitempty"`
	ReadyLanes    []string                            `json:"readyLanes,omitempty"`
	RouteSetup    string                              `json:"routeSetup,omitempty"`
	CancelPhase   string                              `json:"cancelPhase,omitempty"`
}

type crawlerDiscoveredNodesExpected struct {
	Factory  *crawlerDiscoveredNodesFactory     `json:"factory,omitempty"`
	Batching *crawlerDiscoveredNodesBatching    `json:"batching,omitempty"`
	Crawler  *crawlerDiscoveredNodesRunExpected `json:"crawler,omitempty"`
	Source   *crawlerDiscoveredNodesSourceFacts `json:"source,omitempty"`
}

type crawlerDiscoveredNodesFactory struct {
	InputCapacity               int  `json:"inputCapacity"`
	MaxBatchSize                int  `json:"maxBatchSize"`
	TickerIntervalMS            int  `json:"tickerIntervalMs"`
	OutputCapacity              int  `json:"outputCapacity"`
	PingCapacity                int  `json:"pingCapacity"`
	PingConcurrency             int  `json:"pingConcurrency"`
	FindNodeCapacity            int  `json:"findNodeCapacity"`
	FindNodeConcurrency         int  `json:"findNodeConcurrency"`
	SampleInfohashesCapacity    int  `json:"sampleInfohashesCapacity"`
	SampleInfohashesConcurrency int  `json:"sampleInfohashesConcurrency"`
	TimingMeasured              bool `json:"timingMeasured"`
}

type crawlerDiscoveredNodesBatching struct {
	SizeBatch                  []int   `json:"sizeBatch"`
	OutputBatchWhileBlocked    []int   `json:"outputBatchWhileBlocked"`
	HeldBatchWhileBlocked      []int   `json:"heldBatchWhileBlocked"`
	BufferedInputsWhileBlocked []int   `json:"bufferedInputsWhileBlocked"`
	SendWouldBlockBeforeDrain  []int   `json:"sendWouldBlockBeforeDrain"`
	SendCompletedAfterDrain    []int   `json:"sendCompletedAfterDrain"`
	RemainingBatches           [][]int `json:"remainingBatches"`
	Dropped                    int     `json:"dropped"`
}

type crawlerDiscoveredNodesRunExpected struct {
	Batches           []crawlerDiscoveredNodesBatchExpected `json:"batches"`
	Routing           crawlerDiscoveredNodesRouting         `json:"routing"`
	TableMutatorCalls int                                   `json:"tableMutatorCalls"`
	FilterCalls       int                                   `json:"filterCalls"`
	Exited            bool                                  `json:"exited"`
}

type crawlerDiscoveredNodesBatchExpected struct {
	Dedupe       []crawlerDiscoveredNodesDedupe   `json:"dedupe"`
	FilterInput  []crawlerDiscoveredNodesAddress  `json:"filterInput"`
	FilterOutput []crawlerDiscoveredNodesAddress  `json:"filterOutput"`
	Deliveries   []crawlerDiscoveredNodesDelivery `json:"deliveries"`
}

type crawlerDiscoveredNodesSourceFacts struct {
	TickerStartsAtConstruction     bool              `json:"tickerStartsAtConstruction"`
	TickerAndInputSelectUnbiased   bool              `json:"tickerAndInputSelectUnbiased"`
	TickerIntervalIsStrictDeadline bool              `json:"tickerIntervalIsStrictDeadline"`
	TickerFlushesNonemptyBuffer    bool              `json:"tickerFlushesNonemptyBuffer"`
	EmptyTickerDoesNotFlush        bool              `json:"emptyTickerDoesNotFlush"`
	FlushResetsBeforeOutputSend    bool              `json:"flushResetsBeforeOutputSend"`
	InputCloseBreaksOnlySelect     bool              `json:"inputCloseBreaksOnlySelect"`
	InputClosePartialBufferOutcome string            `json:"inputClosePartialBufferOutcome"`
	OutputCloseDeferredUnreached   bool              `json:"outputCloseDeferredUnreached"`
	CrawlerOutputReceiveChecksOK   bool              `json:"crawlerOutputReceiveChecksOk"`
	ClosedOutputCanSpin            bool              `json:"closedOutputCanSpin"`
	RoutingTieOutcome              string            `json:"routingTieOutcome"`
	FilterCancellation             string            `json:"filterCancellation"`
	FilterResultTrust              string            `json:"filterResultTrust"`
	ClosedWorkerSelection          string            `json:"closedWorkerSelection"`
	StateAfterFilter               string            `json:"stateAfterFilter"`
	SourceSHA256                   map[string]string `json:"sourceSha256"`
	Evidence                       string            `json:"evidence"`
}

type crawlerDiscoveredNodesInputNode struct {
	Ordinal int                           `json:"ordinal"`
	ID      string                        `json:"id"`
	Addr    crawlerDiscoveredNodesAddress `json:"addr"`
}

type crawlerDiscoveredNodesAddress struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope"`
}

type crawlerDiscoveredNodesTableSetup struct {
	Kind string                        `json:"kind"`
	ID   string                        `json:"id"`
	Addr crawlerDiscoveredNodesAddress `json:"addr"`
}

type crawlerDiscoveredNodesDedupe struct {
	Key           string `json:"key"`
	WinnerOrdinal int    `json:"winnerOrdinal"`
}

type crawlerDiscoveredNodesDelivery struct {
	Ordinal int    `json:"ordinal"`
	Lane    string `json:"lane"`
}

type crawlerDiscoveredNodesRouting struct {
	AllowedLanes          []string `json:"allowedLanes"`
	ExactlyOnce           bool     `json:"exactlyOnce"`
	PerLanePreservesOrder bool     `json:"perLanePreservesOrder"`
	ReadyTieUnspecified   bool     `json:"readyTieUnspecified"`
	CancelledUndelivered  int      `json:"cancelledUndelivered"`
}

type crawlerDiscoveredNodesFilterTrace struct {
	Input  []netip.Addr
	Output []netip.Addr
}

type crawlerDiscoveredNodesTracingTable struct {
	ktable.Table
	traces       chan crawlerDiscoveredNodesFilterTrace
	mutatorCalls int
}

func (t *crawlerDiscoveredNodesTracingTable) FilterKnownAddrs(addrs []netip.Addr) []netip.Addr {
	input := append([]netip.Addr(nil), addrs...)
	output := t.Table.FilterKnownAddrs(addrs)
	t.traces <- crawlerDiscoveredNodesFilterTrace{
		Input: input, Output: append([]netip.Addr(nil), output...),
	}
	return output
}

func (t *crawlerDiscoveredNodesTracingTable) PutNode(
	id protocol.ID,
	addr netip.AddrPort,
	options ...ktable.NodeOption,
) btree.PutResult {
	t.mutatorCalls++
	return t.Table.PutNode(id, addr, options...)
}

func (t *crawlerDiscoveredNodesTracingTable) DropNode(id protocol.ID, reason error) bool {
	t.mutatorCalls++
	return t.Table.DropNode(id, reason)
}

func (t *crawlerDiscoveredNodesTracingTable) PutHash(
	id protocol.ID,
	peers []ktable.HashPeer,
	options ...ktable.HashOption,
) btree.PutResult {
	t.mutatorCalls++
	return t.Table.PutHash(id, peers, options...)
}

func (t *crawlerDiscoveredNodesTracingTable) BatchCommand(commands ...ktable.Command) {
	t.mutatorCalls++
	t.Table.BatchCommand(commands...)
}

type crawlerDiscoveredNodesManualBatcher struct {
	input  chan ktable.Node
	output chan []ktable.Node
}

func (b *crawlerDiscoveredNodesManualBatcher) In() chan<- ktable.Node {
	return b.input
}

func (b *crawlerDiscoveredNodesManualBatcher) Out() <-chan []ktable.Node {
	return b.output
}

type crawlerDiscoveredNodesLane struct {
	input     chan ktable.Node
	evaluated chan struct{}
}

func (l *crawlerDiscoveredNodesLane) In() chan<- ktable.Node {
	if l.evaluated != nil {
		l.evaluated <- struct{}{}
	}
	return l.input
}

func (*crawlerDiscoveredNodesLane) Run(context.Context, func(ktable.Node)) error {
	panic("oracle routing lane Run must not be called")
}

func TestGenerateDHTCrawlerDiscoveredNodesParity(t *testing.T) {
	fixtures := []crawlerDiscoveredNodesFixture{
		runCrawlerDiscoveredNodesFactoryScenario(t),
		runCrawlerDiscoveredNodesBatchingScenario(t),
		runCrawlerDiscoveredNodesScenario(t, crawlerDiscoveredNodesScenario{
			id: "first_ip_wins_known_filter_and_only_ping_ready",
			batch: []crawlerDiscoveredNodesInputNode{
				crawlerDiscoveredNodesNode(0, "192.0.2.1", 1001, 0),
				crawlerDiscoveredNodesNode(1, "192.0.2.1", 1002, 0),
				crawlerDiscoveredNodesNode(2, "::ffff:192.0.2.1", 1003, 0),
				crawlerDiscoveredNodesNode(3, "2001:db8::3", 1004, 0),
				crawlerDiscoveredNodesNode(4, "fe80::4", 1005, 7),
				crawlerDiscoveredNodesNode(5, "fe80::4", 1006, 7),
				crawlerDiscoveredNodesNode(6, "fe80::4", 1007, 8),
				crawlerDiscoveredNodesNode(7, "198.51.100.7", 1008, 0),
				crawlerDiscoveredNodesNode(8, "198.51.100.8", 1009, 0),
				crawlerDiscoveredNodesNode(9, "198.51.100.9", 1010, 0),
			},
			tableSetup: []crawlerDiscoveredNodesTableSetup{
				crawlerDiscoveredNodesSetup("put_node_once", 70, "198.51.100.7", 7000, 0),
				crawlerDiscoveredNodesSetup("put_node_twice", 80, "198.51.100.8", 8000, 0),
				crawlerDiscoveredNodesSetup("put_hash_peer", 90, "198.51.100.9", 9000, 0),
			},
			readyLanes: []string{"ping"},
		}),
		runCrawlerDiscoveredNodesMultiBatchScenario(t),
		runCrawlerDiscoveredNodesScenario(t, crawlerDiscoveredNodesScenario{
			id: "only_find_node_ready",
			batch: []crawlerDiscoveredNodesInputNode{
				crawlerDiscoveredNodesNode(20, "203.0.113.20", 2020, 0),
				crawlerDiscoveredNodesNode(21, "203.0.113.21", 2021, 0),
			},
			readyLanes: []string{"find_node"},
		}),
		runCrawlerDiscoveredNodesScenario(t, crawlerDiscoveredNodesScenario{
			id: "only_sample_infohashes_ready",
			batch: []crawlerDiscoveredNodesInputNode{
				crawlerDiscoveredNodesNode(30, "203.0.113.30", 3030, 0),
				crawlerDiscoveredNodesNode(31, "203.0.113.31", 3031, 0),
			},
			readyLanes: []string{"sample_infohashes"},
		}),
		runCrawlerDiscoveredNodesScenario(t, crawlerDiscoveredNodesScenario{
			id: "all_ready_routes_each_node_exactly_once",
			batch: []crawlerDiscoveredNodesInputNode{
				crawlerDiscoveredNodesNode(40, "203.0.113.40", 4040, 0),
				crawlerDiscoveredNodesNode(41, "203.0.113.41", 4041, 0),
				crawlerDiscoveredNodesNode(42, "203.0.113.42", 4042, 0),
			},
			readyLanes:        []string{"ping", "find_node", "sample_infohashes"},
			normalizeReadyTie: true,
		}),
		runCrawlerDiscoveredNodesFullRouteScenario(t),
		runCrawlerDiscoveredNodesScenario(t, crawlerDiscoveredNodesScenario{
			id: "cancel_after_one_delivery_abandons_blocked_suffix",
			batch: []crawlerDiscoveredNodesInputNode{
				crawlerDiscoveredNodesNode(45, "203.0.113.45", 4545, 0),
				crawlerDiscoveredNodesNode(46, "203.0.113.46", 4646, 0),
			},
			readyLanes:    []string{"ping"},
			routeCapacity: 1,
			cancelMode:    "after_one_delivery_next_route_blocked",
		}),
		runCrawlerDiscoveredNodesScenario(t, crawlerDiscoveredNodesScenario{
			id: "blocked_route_cancellation_exits_without_delivery",
			batch: []crawlerDiscoveredNodesInputNode{
				crawlerDiscoveredNodesNode(50, "203.0.113.50", 5050, 0),
			},
			cancelMode: "blocked_route_after_filter",
		}),
	}

	if len(fixtures) != len(crawlerDiscoveredNodesFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerDiscoveredNodesFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerDiscoveredNodesFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerDiscoveredNodesFixtureIDs[index])
		}
	}
	reconcileCrawlerDiscoveredNodesFixtures(t, fixtures)
}

func runCrawlerDiscoveredNodesFactoryScenario(t *testing.T) crawlerDiscoveredNodesFixture {
	t.Helper()
	config := NewDefaultConfig()
	result := NewDiscoveredNodes(DiscoveredNodesParams{Config: config})
	value := reflect.ValueOf(result.DiscoveredNodes)
	if value.Kind() != reflect.Ptr {
		t.Fatalf("batching channel kind = %s, want pointer", value.Kind())
	}
	value = value.Elem()
	maxBatchSizeField := value.FieldByName("maxBatchSize")
	maxWaitTimeField := value.FieldByName("maxWaitTime")
	tickerField := value.FieldByName("ticker")
	if !maxBatchSizeField.IsValid() || maxBatchSizeField.Kind() != reflect.Int {
		t.Fatalf("maxBatchSize reflection field is missing or has kind %s", maxBatchSizeField.Kind())
	}
	if !maxWaitTimeField.IsValid() || maxWaitTimeField.Kind() != reflect.Int64 {
		t.Fatalf("maxWaitTime reflection field is missing or has kind %s", maxWaitTimeField.Kind())
	}
	if !tickerField.IsValid() || tickerField.Kind() != reflect.Ptr || tickerField.IsNil() {
		t.Fatalf("ticker reflection field is missing, not a pointer, or nil")
	}
	maxBatchSize := int(maxBatchSizeField.Int())
	maxWaitTime := time.Duration(maxWaitTimeField.Int())
	if maxBatchSize != 10 || maxWaitTime != 10*time.Millisecond {
		t.Fatalf("factory batch config = (%d, %s), want (10, 10ms)", maxBatchSize, maxWaitTime)
	}
	if cap(result.DiscoveredNodes.In()) != 1000 || cap(result.DiscoveredNodes.Out()) != 1 {
		t.Fatalf("factory channel capacities = (%d, %d), want (1000, 1)",
			cap(result.DiscoveredNodes.In()), cap(result.DiscoveredNodes.Out()))
	}
	assertCrawlerDiscoveredNodesCloseSourceShape(t)

	return crawlerDiscoveredNodesFixture{
		ID:        "production_factory_defaults_and_source_lifecycle",
		Subsystem: "dht_crawler_discovered_nodes",
		Oracle: crawlerDiscoveredNodesOracle{
			Composition:       "actual_factory_reflection_plus_go_ast_source_shape",
			Determinism:       "configuration_only_no_wall_clock_measurement_or_busy_loop_execution",
			Ingress:           "actual_production_batching_channel",
			Routes:            "factory_source_defaults_not_live_worker_construction",
			AddressProjection: "netip_address_plus_numeric_zone_port_discarded_textual_zones_and_rust_flowinfo_excluded",
		},
		Input: crawlerDiscoveredNodesInput{Kind: "factory", ScalingFactor: config.ScalingFactor},
		Expected: crawlerDiscoveredNodesExpected{
			Factory: &crawlerDiscoveredNodesFactory{
				InputCapacity: 1000, MaxBatchSize: 10, TickerIntervalMS: 10,
				OutputCapacity: 1,
				PingCapacity:   10, PingConcurrency: 10,
				FindNodeCapacity: 100, FindNodeConcurrency: 100,
				SampleInfohashesCapacity: 100, SampleInfohashesConcurrency: 100,
				TimingMeasured: false,
			},
			Source: &crawlerDiscoveredNodesSourceFacts{
				TickerStartsAtConstruction: true, TickerAndInputSelectUnbiased: true,
				TickerIntervalIsStrictDeadline: false, TickerFlushesNonemptyBuffer: true,
				EmptyTickerDoesNotFlush: true, FlushResetsBeforeOutputSend: true,
				InputCloseBreaksOnlySelect:     true,
				InputClosePartialBufferOutcome: "unspecified_tick_may_or_may_not_win_while_closed_input_spins",
				OutputCloseDeferredUnreached:   true, CrawlerOutputReceiveChecksOK: false,
				ClosedOutputCanSpin:   true,
				RoutingTieOutcome:     "cancel_vs_ready_lane_and_cancel_vs_batch_unspecified",
				FilterCancellation:    "synchronous_filter_call_is_not_cancellation_aware",
				FilterResultTrust:     "returned_order_duplicates_and_unknown_keys_are_trusted",
				ClosedWorkerSelection: "selected_send_to_closed_worker_channel_panics",
				StateAfterFilter:      "not_rechecked_before_route",
				SourceSHA256:          crawlerDiscoveredNodesSourceDigests(t),
				Evidence:              "reflection_plus_go_ast_plus_exact_source_digests_no_busy_loop_execution",
			},
		},
	}
}

func runCrawlerDiscoveredNodesBatchingScenario(t *testing.T) crawlerDiscoveredNodesFixture {
	t.Helper()
	sized := concurrency.NewBatchingChannel[int](1000, 10, time.Hour)
	values := make([]int, 10)
	for index := range values {
		values[index] = index
		sized.In() <- index
	}
	sizeBatch := crawlerDiscoveredNodesReceiveIntBatch(t, sized.Out())
	if !reflect.DeepEqual(sizeBatch, values) {
		t.Fatalf("size batch = %v, want %v", sizeBatch, values)
	}

	blocked := concurrency.NewBatchingChannel[int](2, 1, time.Hour)
	blocked.In() <- 1
	crawlerDiscoveredNodesWaitFor(t, func() bool { return len(blocked.Out()) == 1 }, "first output batch")
	blocked.In() <- 2
	crawlerDiscoveredNodesWaitFor(t, func() bool { return len(blocked.In()) == 0 }, "worker-held second batch")
	blocked.In() <- 3
	blocked.In() <- 4
	if len(blocked.In()) != cap(blocked.In()) {
		t.Fatalf("blocked input length = %d, want capacity %d", len(blocked.In()), cap(blocked.In()))
	}
	select {
	case blocked.In() <- 5:
		t.Fatal("nonblocking send completed while output and input were both full")
	default:
	}
	firstOutput := crawlerDiscoveredNodesReceiveIntBatch(t, blocked.Out())
	crawlerDiscoveredNodesWaitFor(t, func() bool {
		return len(blocked.In()) < cap(blocked.In())
	}, "input slot after output drain")
	blocked.In() <- 5
	remaining := make([][]int, 0, 4)
	for range 4 {
		remaining = append(remaining, crawlerDiscoveredNodesReceiveIntBatch(t, blocked.Out()))
	}
	wantRemaining := [][]int{{2}, {3}, {4}, {5}}
	if !reflect.DeepEqual(remaining, wantRemaining) {
		t.Fatalf("remaining batches = %v, want %v", remaining, wantRemaining)
	}

	return crawlerDiscoveredNodesFixture{
		ID:        "size_flush_order_and_output_backpressure",
		Subsystem: "dht_crawler_discovered_nodes",
		Oracle: crawlerDiscoveredNodesOracle{
			Composition:       "actual_generic_batching_channel",
			Determinism:       "one_hour_ticker_isolates_size_and_capacity_handshakes",
			Ingress:           "actual_buffered_input",
			Routes:            "actual_single_slot_batch_output",
			AddressProjection: "not_applicable",
		},
		Input: crawlerDiscoveredNodesInput{Kind: "batching", Values: values},
		Expected: crawlerDiscoveredNodesExpected{
			Batching: &crawlerDiscoveredNodesBatching{
				SizeBatch: sizeBatch, OutputBatchWhileBlocked: firstOutput,
				HeldBatchWhileBlocked: []int{2}, BufferedInputsWhileBlocked: []int{3, 4},
				SendWouldBlockBeforeDrain: []int{5}, SendCompletedAfterDrain: []int{5},
				RemainingBatches: remaining, Dropped: 0,
			},
		},
	}
}

func runCrawlerDiscoveredNodesMultiBatchScenario(t *testing.T) crawlerDiscoveredNodesFixture {
	t.Helper()
	batches := [][]crawlerDiscoveredNodesInputNode{
		{crawlerDiscoveredNodesNode(60, "203.0.113.60", 6060, 0)},
		{crawlerDiscoveredNodesNode(61, "203.0.113.61", 6161, 0)},
		{crawlerDiscoveredNodesNode(62, "203.0.113.60", 6262, 0)},
	}
	setup := []crawlerDiscoveredNodesTableSetup{
		crawlerDiscoveredNodesSetup("put_hash_peer", 160, "203.0.113.61", 9999, 0),
	}
	base := ktable.New(ktable.Params{NodeID: protocol.ID{}}).Table
	applyCrawlerDiscoveredNodesTableSetup(t, base, setup)
	tracingTable := &crawlerDiscoveredNodesTracingTable{
		Table: base, traces: make(chan crawlerDiscoveredNodesFilterTrace, len(batches)),
	}
	batcher := &crawlerDiscoveredNodesManualBatcher{
		input: make(chan ktable.Node), output: make(chan []ktable.Node, len(batches)),
	}
	ping := &crawlerDiscoveredNodesLane{input: make(chan ktable.Node, len(batches)+1)}
	findNode := &crawlerDiscoveredNodesLane{}
	sample := &crawlerDiscoveredNodesLane{}
	c := crawler{
		kTable: tracingTable, discoveredNodes: batcher,
		nodesForPing: ping, nodesForFindNode: findNode, nodesForSampleInfoHashes: sample,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.runDiscoveredNodes(ctx)
	}()

	expectedBatches := make([]crawlerDiscoveredNodesBatchExpected, 0, len(batches))
	for index, inputBatch := range batches {
		batcher.output <- crawlerDiscoveredNodesNodes(t, inputBatch)
		trace := crawlerDiscoveredNodesReceiveTrace(t, tracingTable.traces, "multi-batch filter")
		scenario := crawlerDiscoveredNodesScenario{
			id: fmt.Sprintf("multi-batch-%d", index), batch: inputBatch, readyLanes: []string{"ping"},
		}
		deliveries := collectCrawlerDiscoveredNodesDeliveries(t, scenario, map[string]*crawlerDiscoveredNodesLane{
			"ping": ping, "find_node": findNode, "sample_infohashes": sample,
		}, len(trace.Output))
		expectedBatches = append(
			expectedBatches,
			crawlerDiscoveredNodesExpectedBatch(t, inputBatch, trace, deliveries),
		)
	}
	cancel()
	crawlerDiscoveredNodesWaitDone(t, done, "multi-batch crawler cancellation")

	return crawlerDiscoveredNodesFixture{
		ID:        "cross_batch_dedupe_resets_and_all_known_continues",
		Subsystem: "dht_crawler_discovered_nodes",
		Oracle: crawlerDiscoveredNodesOracle{
			Composition:       "actual_run_discovered_nodes_plus_traced_actual_ktable_manual_batcher_and_readiness_controlled_lane_adapters",
			Determinism:       "three_sequential_batches_with_forced_ping_lane",
			Ingress:           "scripted_complete_batches_not_production_batching_timing",
			Routes:            "manual_channels_not_production_buffered_concurrent_workers",
			AddressProjection: "netip_address_plus_numeric_zone_port_discarded_textual_zones_and_rust_flowinfo_excluded",
		},
		Input: crawlerDiscoveredNodesInput{
			Kind: "crawler", Batches: batches, TableSetup: setup,
			ReadyLanes: []string{"ping"}, CancelPhase: "after_all_deliveries",
		},
		Expected: crawlerDiscoveredNodesExpected{Crawler: &crawlerDiscoveredNodesRunExpected{
			Batches: expectedBatches,
			Routing: crawlerDiscoveredNodesRouting{
				AllowedLanes: []string{"ping"}, ExactlyOnce: true,
				PerLanePreservesOrder: true, ReadyTieUnspecified: false,
				CancelledUndelivered: 0,
			},
			TableMutatorCalls: tracingTable.mutatorCalls,
			FilterCalls:       len(batches),
			Exited:            true,
		}},
	}
}

func runCrawlerDiscoveredNodesFullRouteScenario(t *testing.T) crawlerDiscoveredNodesFixture {
	t.Helper()
	inputBatch := []crawlerDiscoveredNodesInputNode{
		crawlerDiscoveredNodesNode(70, "203.0.113.70", 7070, 0),
	}
	base := ktable.New(ktable.Params{NodeID: protocol.ID{}}).Table
	tracingTable := &crawlerDiscoveredNodesTracingTable{
		Table: base, traces: make(chan crawlerDiscoveredNodesFilterTrace, 1),
	}
	batcher := &crawlerDiscoveredNodesManualBatcher{
		input: make(chan ktable.Node), output: make(chan []ktable.Node, 1),
	}
	evaluations := make(chan struct{}, 4)
	lanes := map[string]*crawlerDiscoveredNodesLane{
		"ping":              {input: make(chan ktable.Node, 1), evaluated: evaluations},
		"find_node":         {input: make(chan ktable.Node, 1)},
		"sample_infohashes": {input: make(chan ktable.Node, 1)},
	}
	lanes["ping"].input <- ktable.NewNode(
		protocol.MustParseID(crawlerDiscoveredNodesID(900)), netip.MustParseAddrPort("192.0.2.200:9000"),
	)
	lanes["find_node"].input <- ktable.NewNode(
		protocol.MustParseID(crawlerDiscoveredNodesID(901)), netip.MustParseAddrPort("192.0.2.201:9001"),
	)
	lanes["sample_infohashes"].input <- ktable.NewNode(
		protocol.MustParseID(crawlerDiscoveredNodesID(902)), netip.MustParseAddrPort("192.0.2.202:9002"),
	)
	c := crawler{
		kTable: tracingTable, discoveredNodes: batcher,
		nodesForPing: lanes["ping"], nodesForFindNode: lanes["find_node"],
		nodesForSampleInfoHashes: lanes["sample_infohashes"],
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.runDiscoveredNodes(ctx)
	}()
	batcher.output <- crawlerDiscoveredNodesNodes(t, inputBatch)
	trace := crawlerDiscoveredNodesReceiveTrace(t, tracingTable.traces, "full-route filter")
	crawlerDiscoveredNodesReceiveEvaluation(t, evaluations, "all-full route")
	for name, lane := range lanes {
		if len(lane.input) != 1 {
			t.Fatalf("full-route lane %s length = %d, want 1", name, len(lane.input))
		}
	}
	<-lanes["ping"].input
	crawlerDiscoveredNodesWaitFor(t, func() bool { return len(lanes["ping"].input) == 1 }, "ping route after drain")
	routed := <-lanes["ping"].input
	if routed.ID().String() != inputBatch[0].ID {
		t.Fatalf("full-route delivery id = %s, want %s", routed.ID(), inputBatch[0].ID)
	}
	<-lanes["find_node"].input
	<-lanes["sample_infohashes"].input
	cancel()
	crawlerDiscoveredNodesWaitDone(t, done, "full-route crawler cancellation")
	deliveries := []crawlerDiscoveredNodesDelivery{{Ordinal: inputBatch[0].Ordinal, Lane: "ping"}}

	return crawlerDiscoveredNodesFixture{
		ID:        "all_full_then_ping_drain_routes",
		Subsystem: "dht_crawler_discovered_nodes",
		Oracle: crawlerDiscoveredNodesOracle{
			Composition:       "actual_run_discovered_nodes_plus_traced_actual_ktable_manual_batcher_and_readiness_controlled_lane_adapters",
			Determinism:       "all_routes_full_barrier_then_only_ping_capacity_released",
			Ingress:           "scripted_complete_batch_not_production_batching_timing",
			Routes:            "manual_bounded_channels_not_production_buffered_concurrent_workers",
			AddressProjection: "netip_address_plus_numeric_zone_port_discarded_textual_zones_and_rust_flowinfo_excluded",
		},
		Input: crawlerDiscoveredNodesInput{
			Kind: "crawler", Batches: [][]crawlerDiscoveredNodesInputNode{inputBatch},
			RouteSetup:  "all_three_capacity_one_prefilled_then_ping_drained",
			CancelPhase: "after_all_deliveries",
		},
		Expected: crawlerDiscoveredNodesExpected{Crawler: &crawlerDiscoveredNodesRunExpected{
			Batches: []crawlerDiscoveredNodesBatchExpected{
				crawlerDiscoveredNodesExpectedBatch(t, inputBatch, trace, deliveries),
			},
			Routing: crawlerDiscoveredNodesRouting{
				AllowedLanes: []string{"ping"}, ExactlyOnce: true,
				PerLanePreservesOrder: true, ReadyTieUnspecified: false,
				CancelledUndelivered: 0,
			},
			TableMutatorCalls: tracingTable.mutatorCalls,
			FilterCalls:       1,
			Exited:            true,
		}},
	}
}

type crawlerDiscoveredNodesScenario struct {
	id                string
	batch             []crawlerDiscoveredNodesInputNode
	tableSetup        []crawlerDiscoveredNodesTableSetup
	readyLanes        []string
	normalizeReadyTie bool
	routeCapacity     int
	cancelMode        string
}

func runCrawlerDiscoveredNodesScenario(
	t *testing.T,
	scenario crawlerDiscoveredNodesScenario,
) crawlerDiscoveredNodesFixture {
	t.Helper()
	base := ktable.New(ktable.Params{NodeID: protocol.ID{}}).Table
	applyCrawlerDiscoveredNodesTableSetup(t, base, scenario.tableSetup)
	batch := make([]ktable.Node, 0, len(scenario.batch))
	for _, input := range scenario.batch {
		addr := input.Addr.addrPort(t)
		batch = append(batch, ktable.NewNode(protocol.MustParseID(input.ID), addr))
	}

	tracingTable := &crawlerDiscoveredNodesTracingTable{
		Table: base, traces: make(chan crawlerDiscoveredNodesFilterTrace, 1),
	}
	batcher := &crawlerDiscoveredNodesManualBatcher{
		input: make(chan ktable.Node), output: make(chan []ktable.Node, 1),
	}
	lanes := map[string]*crawlerDiscoveredNodesLane{
		"ping":              {},
		"find_node":         {},
		"sample_infohashes": {},
	}
	evaluations := make(chan struct{}, 8)
	if scenario.cancelMode != "" {
		lanes["ping"].evaluated = evaluations
	}
	for _, lane := range scenario.readyLanes {
		capacity := scenario.routeCapacity
		if capacity == 0 {
			capacity = len(scenario.batch) + 1
		}
		lanes[lane].input = make(chan ktable.Node, capacity)
	}
	c := crawler{
		kTable: tracingTable, discoveredNodes: batcher,
		nodesForPing: lanes["ping"], nodesForFindNode: lanes["find_node"],
		nodesForSampleInfoHashes: lanes["sample_infohashes"],
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.runDiscoveredNodes(ctx)
	}()
	batcher.output <- batch
	var trace crawlerDiscoveredNodesFilterTrace
	select {
	case trace = <-tracingTable.traces:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: filter was not reached", scenario.id)
	}

	deliveries := []crawlerDiscoveredNodesDelivery{}
	switch scenario.cancelMode {
	case "blocked_route_after_filter":
		crawlerDiscoveredNodesReceiveEvaluation(t, evaluations, scenario.id)
		cancel()
	case "after_one_delivery_next_route_blocked":
		crawlerDiscoveredNodesWaitFor(t, func() bool {
			return len(lanes["ping"].input) == 1
		}, scenario.id+" first committed delivery")
		crawlerDiscoveredNodesReceiveEvaluation(t, evaluations, scenario.id+" first route")
		crawlerDiscoveredNodesReceiveEvaluation(t, evaluations, scenario.id+" blocked second route")
		cancel()
	case "":
		deliveries = collectCrawlerDiscoveredNodesDeliveries(
			t, scenario, lanes, len(trace.Output),
		)
		cancel()
	default:
		t.Fatalf("%s: unknown cancel mode %q", scenario.id, scenario.cancelMode)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: crawler did not exit after cancellation", scenario.id)
	}
	if scenario.cancelMode == "after_one_delivery_next_route_blocked" {
		deliveries = collectCrawlerDiscoveredNodesDeliveries(t, scenario, lanes, 1)
	}
	for name, lane := range lanes {
		if lane.input != nil && len(lane.input) != 0 {
			t.Fatalf("%s: lane %s retained %d uncollected nodes", scenario.id, name, len(lane.input))
		}
	}
	dedupe := make([]crawlerDiscoveredNodesDedupe, 0, len(trace.Input))
	for _, addr := range trace.Input {
		key := addr.String()
		winner := -1
		for _, input := range scenario.batch {
			if input.Addr.addrPort(t).Addr().String() == key {
				winner = input.Ordinal
				break
			}
		}
		if winner < 0 {
			t.Fatalf("%s: no input winner for filter key %q", scenario.id, key)
		}
		dedupe = append(dedupe, crawlerDiscoveredNodesDedupe{Key: key, WinnerOrdinal: winner})
	}

	allowed := append([]string(nil), scenario.readyLanes...)
	if allowed == nil {
		allowed = []string{}
	}
	sort.Strings(allowed)
	return crawlerDiscoveredNodesFixture{
		ID:        scenario.id,
		Subsystem: "dht_crawler_discovered_nodes",
		Oracle: crawlerDiscoveredNodesOracle{
			Composition:       "actual_run_discovered_nodes_plus_traced_actual_ktable_manual_batcher_and_readiness_controlled_lane_adapters",
			Determinism:       "forced_ready_lanes_or_normalized_ready_tie_with_barrier_cancellation",
			Ingress:           "scripted_complete_batch_not_production_batching_timing",
			Routes:            "manual_channels_not_production_buffered_concurrent_workers",
			AddressProjection: "netip_address_plus_numeric_zone_port_discarded_textual_zones_and_rust_flowinfo_excluded",
		},
		Input: crawlerDiscoveredNodesInput{
			Kind: "crawler", Batches: [][]crawlerDiscoveredNodesInputNode{scenario.batch},
			TableSetup: scenario.tableSetup,
			ReadyLanes: scenario.readyLanes,
			CancelPhase: func() string {
				if scenario.cancelMode != "" {
					return scenario.cancelMode
				}
				return "after_all_deliveries"
			}(),
		},
		Expected: crawlerDiscoveredNodesExpected{
			Crawler: &crawlerDiscoveredNodesRunExpected{
				Batches: []crawlerDiscoveredNodesBatchExpected{{
					Dedupe:       dedupe,
					FilterInput:  projectCrawlerDiscoveredNodesAddrs(trace.Input),
					FilterOutput: projectCrawlerDiscoveredNodesAddrs(trace.Output),
					Deliveries:   deliveries,
				}},
				Routing: crawlerDiscoveredNodesRouting{
					AllowedLanes: allowed, ExactlyOnce: scenario.cancelMode == "",
					PerLanePreservesOrder: true, ReadyTieUnspecified: scenario.normalizeReadyTie,
					CancelledUndelivered: len(trace.Output) - len(deliveries),
				},
				TableMutatorCalls: tracingTable.mutatorCalls,
				FilterCalls:       1,
				Exited:            true,
			},
		},
	}
}

func collectCrawlerDiscoveredNodesDeliveries(
	t *testing.T,
	scenario crawlerDiscoveredNodesScenario,
	lanes map[string]*crawlerDiscoveredNodesLane,
	want int,
) []crawlerDiscoveredNodesDelivery {
	t.Helper()
	crawlerDiscoveredNodesWaitFor(t, func() bool {
		total := 0
		for _, lane := range lanes {
			if lane.input != nil {
				total += len(lane.input)
			}
		}
		return total == want
	}, scenario.id+" deliveries")

	inputOrdinals := make(map[string]int, len(scenario.batch))
	for _, input := range scenario.batch {
		inputOrdinals[input.ID] = input.Ordinal
	}
	deliveries := make([]crawlerDiscoveredNodesDelivery, 0, want)
	seen := make(map[int]struct{}, want)
	for name, lane := range lanes {
		lastOrdinal := -1
		for lane.input != nil && len(lane.input) > 0 {
			node := <-lane.input
			ordinal, ok := inputOrdinals[node.ID().String()]
			if !ok {
				t.Fatalf("%s: delivered unknown node %s", scenario.id, node.ID())
			}
			if ordinal <= lastOrdinal {
				t.Fatalf("%s: lane %s order moved from %d to %d", scenario.id, name, lastOrdinal, ordinal)
			}
			lastOrdinal = ordinal
			if _, duplicate := seen[ordinal]; duplicate {
				t.Fatalf("%s: ordinal %d delivered more than once", scenario.id, ordinal)
			}
			seen[ordinal] = struct{}{}
			laneName := name
			if scenario.normalizeReadyTie {
				laneName = "one_of_ready"
			}
			deliveries = append(deliveries, crawlerDiscoveredNodesDelivery{Ordinal: ordinal, Lane: laneName})
		}
	}
	if len(deliveries) != want {
		t.Fatalf("%s: deliveries = %d, want %d", scenario.id, len(deliveries), want)
	}
	if scenario.normalizeReadyTie {
		sort.Slice(deliveries, func(i, j int) bool { return deliveries[i].Ordinal < deliveries[j].Ordinal })
	}
	return deliveries
}

func applyCrawlerDiscoveredNodesTableSetup(
	t *testing.T,
	table ktable.Table,
	setup []crawlerDiscoveredNodesTableSetup,
) {
	t.Helper()
	for _, operation := range setup {
		id := protocol.MustParseID(operation.ID)
		addr := operation.Addr.addrPort(t)
		switch operation.Kind {
		case "put_node_once":
			table.PutNode(id, addr)
		case "put_node_twice":
			table.PutNode(id, addr)
			table.PutNode(id, addr)
		case "put_hash_peer":
			table.PutHash(id, []ktable.HashPeer{{Addr: addr}})
		default:
			t.Fatalf("unknown table setup kind %q", operation.Kind)
		}
	}
}

func assertCrawlerDiscoveredNodesCloseSourceShape(t *testing.T) {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve discovered-nodes generator source")
	}
	batchingPath := filepath.Clean(filepath.Join(filepath.Dir(source), "../concurrency/batching_channel.go"))
	batchingFile, err := parser.ParseFile(token.NewFileSet(), batchingPath, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	closedInputBreakFound := false
	deferredOutputCloseFound := false
	twoCaseInputTickerSelectFound := false
	tickerGuardedFlushFound := false
	resetBeforeOutputSendFound := false
	for _, declaration := range batchingFile.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == "flush" {
			resetIndex := -1
			sendIndex := -1
			for index, statement := range fn.Body.List {
				switch statement := statement.(type) {
				case *ast.ExprStmt:
					call, ok := statement.X.(*ast.CallExpr)
					if !ok {
						continue
					}
					selector, ok := call.Fun.(*ast.SelectorExpr)
					if ok && selector.Sel.Name == "Reset" {
						resetIndex = index
					}
				case *ast.SendStmt:
					sendIndex = index
				}
			}
			resetBeforeOutputSendFound = resetIndex >= 0 && sendIndex > resetIndex
			continue
		}
		if fn.Name.Name != "batch" {
			continue
		}
		for _, statement := range fn.Body.List {
			if deferred, ok := statement.(*ast.DeferStmt); ok {
				if identifier, ok := deferred.Call.Fun.(*ast.Ident); ok && identifier.Name == "close" {
					deferredOutputCloseFound = true
				}
			}
			loop, ok := statement.(*ast.ForStmt)
			if !ok || loop.Cond != nil {
				continue
			}
			for _, loopStatement := range loop.Body.List {
				selection, ok := loopStatement.(*ast.SelectStmt)
				if !ok {
					continue
				}
				inputClauseFound := false
				tickerClauseFound := false
				for _, clauseStatement := range selection.Body.List {
					clause, ok := clauseStatement.(*ast.CommClause)
					if !ok {
						continue
					}
					if expression, ok := clause.Comm.(*ast.ExprStmt); ok {
						if receive, ok := expression.X.(*ast.UnaryExpr); ok && receive.Op == token.ARROW {
							if selector, ok := receive.X.(*ast.SelectorExpr); ok && selector.Sel.Name == "C" {
								tickerClauseFound = true
								tickerGuardedFlushFound = crawlerDiscoveredNodesHasNonemptyFlushGuard(clause.Body)
							}
						}
					}
					assignment, ok := clause.Comm.(*ast.AssignStmt)
					if !ok || len(assignment.Lhs) != 2 {
						continue
					}
					open, ok := assignment.Lhs[1].(*ast.Ident)
					if !ok || open.Name != "ok" {
						continue
					}
					inputClauseFound = true
					ast.Inspect(&ast.BlockStmt{List: clause.Body}, func(node ast.Node) bool {
						conditional, ok := node.(*ast.IfStmt)
						if !ok {
							return true
						}
						negation, ok := conditional.Cond.(*ast.UnaryExpr)
						if !ok || negation.Op != token.NOT {
							return true
						}
						identifier, ok := negation.X.(*ast.Ident)
						if !ok || identifier.Name != "ok" {
							return true
						}
						for _, bodyStatement := range conditional.Body.List {
							branch, ok := bodyStatement.(*ast.BranchStmt)
							if ok && branch.Tok == token.BREAK && branch.Label == nil {
								closedInputBreakFound = true
							}
						}
						return true
					})
				}
				twoCaseInputTickerSelectFound = len(selection.Body.List) == 2 &&
					inputClauseFound && tickerClauseFound
			}
		}
	}
	if !closedInputBreakFound || !deferredOutputCloseFound ||
		!twoCaseInputTickerSelectFound || !tickerGuardedFlushFound ||
		!resetBeforeOutputSendFound {
		t.Fatalf(
			"batching source shape changed: closed break=%t deferred close=%t two-case select=%t guarded tick=%t reset-before-send=%t",
			closedInputBreakFound,
			deferredOutputCloseFound,
			twoCaseInputTickerSelectFound,
			tickerGuardedFlushFound,
			resetBeforeOutputSendFound,
		)
	}

	crawlerPath := filepath.Clean(filepath.Join(filepath.Dir(source), "discovered_nodes.go"))
	crawlerFile, err := parser.ParseFile(token.NewFileSet(), crawlerPath, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	outReceiveWithOneLHSInsideInfiniteSelect := false
	for _, declaration := range crawlerFile.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "runDiscoveredNodes" {
			continue
		}
		for _, statement := range fn.Body.List {
			loop, ok := statement.(*ast.ForStmt)
			if !ok || loop.Cond != nil {
				continue
			}
			ast.Inspect(loop.Body, func(node ast.Node) bool {
				assign, ok := node.(*ast.AssignStmt)
				if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
					return true
				}
				unary, ok := assign.Rhs[0].(*ast.UnaryExpr)
				if !ok || unary.Op != token.ARROW {
					return true
				}
				call, ok := unary.X.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if ok && selector.Sel.Name == "Out" {
					outReceiveWithOneLHSInsideInfiniteSelect = true
				}
				return true
			})
		}
	}
	if !outReceiveWithOneLHSInsideInfiniteSelect {
		t.Fatal("crawler output receive no longer omits the channel-open boolean")
	}
}

func crawlerDiscoveredNodesHasNonemptyFlushGuard(statements []ast.Stmt) bool {
	for _, statement := range statements {
		conditional, ok := statement.(*ast.IfStmt)
		if !ok {
			continue
		}
		comparison, ok := conditional.Cond.(*ast.BinaryExpr)
		if !ok || comparison.Op != token.GTR {
			continue
		}
		lengthCall, ok := comparison.X.(*ast.CallExpr)
		if !ok {
			continue
		}
		length, ok := lengthCall.Fun.(*ast.Ident)
		if !ok || length.Name != "len" {
			continue
		}
		zero, ok := comparison.Y.(*ast.BasicLit)
		if !ok || zero.Value != "0" {
			continue
		}
		for _, bodyStatement := range conditional.Body.List {
			expression, ok := bodyStatement.(*ast.ExprStmt)
			if !ok {
				continue
			}
			call, ok := expression.X.(*ast.CallExpr)
			if !ok {
				continue
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "flush" {
				return true
			}
		}
	}
	return false
}

func crawlerDiscoveredNodesSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve discovered-nodes generator source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
	paths := map[string]string{
		"internal/concurrency/batching_channel.go": filepath.Join(root, "internal/concurrency/batching_channel.go"),
		"internal/dhtcrawler/config.go":            filepath.Join(root, "internal/dhtcrawler/config.go"),
		"internal/dhtcrawler/crawler.go":           filepath.Join(root, "internal/dhtcrawler/crawler.go"),
		"internal/dhtcrawler/discovered_nodes.go":  filepath.Join(root, "internal/dhtcrawler/discovered_nodes.go"),
		"internal/dhtcrawler/factory.go":           filepath.Join(root, "internal/dhtcrawler/factory.go"),
	}
	digests := make(map[string]string, len(paths))
	for name, path := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(contents)
		digests[name] = fmt.Sprintf("%x", digest)
	}
	return digests
}

func crawlerDiscoveredNodesNodes(
	t *testing.T,
	inputs []crawlerDiscoveredNodesInputNode,
) []ktable.Node {
	t.Helper()
	nodes := make([]ktable.Node, 0, len(inputs))
	for _, input := range inputs {
		nodes = append(nodes, ktable.NewNode(
			protocol.MustParseID(input.ID), input.Addr.addrPort(t),
		))
	}
	return nodes
}

func crawlerDiscoveredNodesReceiveTrace(
	t *testing.T,
	traces <-chan crawlerDiscoveredNodesFilterTrace,
	description string,
) crawlerDiscoveredNodesFilterTrace {
	t.Helper()
	select {
	case trace := <-traces:
		return trace
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return crawlerDiscoveredNodesFilterTrace{}
	}
}

func crawlerDiscoveredNodesExpectedBatch(
	t *testing.T,
	inputs []crawlerDiscoveredNodesInputNode,
	trace crawlerDiscoveredNodesFilterTrace,
	deliveries []crawlerDiscoveredNodesDelivery,
) crawlerDiscoveredNodesBatchExpected {
	t.Helper()
	dedupe := make([]crawlerDiscoveredNodesDedupe, 0, len(trace.Input))
	for _, addr := range trace.Input {
		key := addr.String()
		winner := -1
		for _, input := range inputs {
			if input.Addr.addrPort(t).Addr().String() == key {
				winner = input.Ordinal
				break
			}
		}
		if winner < 0 {
			t.Fatalf("no input winner for filter key %q", key)
		}
		dedupe = append(dedupe, crawlerDiscoveredNodesDedupe{Key: key, WinnerOrdinal: winner})
	}
	if deliveries == nil {
		deliveries = []crawlerDiscoveredNodesDelivery{}
	}
	return crawlerDiscoveredNodesBatchExpected{
		Dedupe:       dedupe,
		FilterInput:  projectCrawlerDiscoveredNodesAddrs(trace.Input),
		FilterOutput: projectCrawlerDiscoveredNodesAddrs(trace.Output),
		Deliveries:   deliveries,
	}
}

func crawlerDiscoveredNodesWaitDone(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func crawlerDiscoveredNodesNode(
	ordinal int,
	ip string,
	port uint16,
	scope uint32,
) crawlerDiscoveredNodesInputNode {
	return crawlerDiscoveredNodesInputNode{
		Ordinal: ordinal, ID: crawlerDiscoveredNodesID(ordinal + 1),
		Addr: crawlerDiscoveredNodesAddress{IP: ip, Port: port, Scope: scope},
	}
}

func crawlerDiscoveredNodesSetup(
	kind string,
	id int,
	ip string,
	port uint16,
	scope uint32,
) crawlerDiscoveredNodesTableSetup {
	return crawlerDiscoveredNodesTableSetup{
		Kind: kind, ID: crawlerDiscoveredNodesID(id),
		Addr: crawlerDiscoveredNodesAddress{IP: ip, Port: port, Scope: scope},
	}
}

func crawlerDiscoveredNodesID(value int) string {
	var id protocol.ID
	id[18] = byte(value >> 8)
	id[19] = byte(value)
	return id.String()
}

func (a crawlerDiscoveredNodesAddress) addrPort(t *testing.T) netip.AddrPort {
	t.Helper()
	addr, err := netip.ParseAddr(a.IP)
	if err != nil {
		t.Fatal(err)
	}
	if a.Scope != 0 {
		addr = addr.WithZone(strconv.FormatUint(uint64(a.Scope), 10))
	}
	return netip.AddrPortFrom(addr, a.Port)
}

func projectCrawlerDiscoveredNodesAddrs(addrs []netip.Addr) []crawlerDiscoveredNodesAddress {
	projected := make([]crawlerDiscoveredNodesAddress, 0, len(addrs))
	for _, addr := range addrs {
		scope, _ := strconv.ParseUint(addr.Zone(), 10, 32)
		projected = append(projected, crawlerDiscoveredNodesAddress{
			IP: addr.WithZone("").String(), Scope: uint32(scope),
		})
	}
	return projected
}

func crawlerDiscoveredNodesReceiveIntBatch(t *testing.T, out <-chan []int) []int {
	t.Helper()
	select {
	case batch := <-out:
		return append([]int(nil), batch...)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for batching-channel output")
		return nil
	}
}

func crawlerDiscoveredNodesReceiveEvaluation(
	t *testing.T,
	evaluations <-chan struct{},
	description string,
) {
	t.Helper()
	select {
	case <-evaluations:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s routing evaluation", description)
	}
}

func crawlerDiscoveredNodesWaitFor(t *testing.T, predicate func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		runtime.Gosched()
	}
}

func reconcileCrawlerDiscoveredNodesFixtures(
	t *testing.T,
	fixtures []crawlerDiscoveredNodesFixture,
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
	fixtureHash := sha256.Sum256(encoded.Bytes())
	actualHash := fmt.Sprintf("%x", fixtureHash)
	if crawlerDiscoveredNodesFixtureSHA256 != "" && actualHash != crawlerDiscoveredNodesFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerDiscoveredNodesFixtureSHA256)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve discovered-nodes generator source")
	}
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source), "../../testdata/parity/dht/dht_crawler_discovered_nodes.jsonl",
	))
	if *updateDHTCrawlerDiscoveredNodesParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-discovered-nodes-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler discovered-nodes fixture is stale; rerun with -update-dht-crawler-discovered-nodes-parity")
	}
}
