package dhtcrawler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var updateDHTCrawlerBootstrapPingProducerParity = flag.Bool(
	"update-dht-crawler-bootstrap-ping-producer-parity",
	false,
	"rewrite the Rust DHT crawler bootstrap-ping-producer parity fixture",
)

const crawlerBootstrapPingProducerFixtureSHA256 = "663339a94b6efaaa626c97f5dfb357d3343d51e5c00f2841d638804590fdefbe"

var crawlerBootstrapPingProducerFixtureIDs = [...]string{
	"production_source_factory_defaults_and_lifecycle_contract",
	"ordered_numeric_ipv4_ipv6_delivery_then_cancel_before_second_round",
	"malformed_address_warns_and_continues_to_later_valid_address",
	"ordered_prefix_then_cancel_at_blocked_third_ping_send",
}

type crawlerBootstrapPingProducerFixture struct {
	ID             string                               `json:"id"`
	Subsystem      string                               `json:"subsystem"`
	Classification string                               `json:"classification"`
	Oracle         crawlerBootstrapPingProducerOracle   `json:"oracle"`
	Input          crawlerBootstrapPingProducerInput    `json:"input"`
	Expected       crawlerBootstrapPingProducerExpected `json:"expected"`
}

type crawlerBootstrapPingProducerOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Resolver    string `json:"resolver"`
	Lane        string `json:"lane"`
	Timer       string `json:"timer"`
}

type crawlerBootstrapPingProducerInput struct {
	Kind                       string   `json:"kind"`
	ContextInitiallyCancelled  bool     `json:"contextInitiallyCancelled"`
	InitialIntervalMS          int      `json:"initialIntervalMs"`
	ConfiguredReseedIntervalMS int      `json:"configuredReseedIntervalMs"`
	EffectiveReseedIntervalMS  int      `json:"effectiveReseedIntervalMs"`
	BootstrapNodes             []string `json:"bootstrapNodes"`
	LaneCapacity               int      `json:"laneCapacity"`
	CancelAtLaneInCall         int      `json:"cancelAtLaneInCall"`
}

type crawlerBootstrapPingProducerExpected struct {
	LaneInCalls       int                                    `json:"laneInCalls"`
	Deliveries        []crawlerBootstrapPingProducerDelivery `json:"deliveries"`
	ResolutionSkipped []string                               `json:"resolutionSkipped"`
	Abandoned         []string                               `json:"abandoned"`
	Warnings          []string                               `json:"warnings"`
	Events            []string                               `json:"events"`
	RunReturned       bool                                   `json:"runReturned"`
	ContextCancelled  bool                                   `json:"contextCancelled"`
	Source            *crawlerBootstrapPingProducerSource    `json:"source,omitempty"`
}

type crawlerBootstrapPingProducerDelivery struct {
	Configured                string `json:"configured"`
	ID                        string `json:"id"`
	Addr                      string `json:"addr"`
	TimeIsZero                bool   `json:"timeIsZero"`
	Dropped                   bool   `json:"dropped"`
	SampleInfoHashesCandidate bool   `json:"sampleInfohashesCandidate"`
}

type crawlerBootstrapPingProducerSource struct {
	InitialIntervalMS                      int               `json:"initialIntervalMs"`
	InitialTimerCancellationAware          bool              `json:"initialTimerCancellationAware"`
	ReadyInitialTimerCancelOutcome         string            `json:"readyInitialTimerCancelOutcome"`
	ResolvesSequentially                   bool              `json:"resolvesSequentially"`
	ResolverNetwork                        string            `json:"resolverNetwork"`
	ResolutionCancellationAware            bool              `json:"resolutionCancellationAware"`
	ResolutionErrorWarnsAndContinues       bool              `json:"resolutionErrorWarnsAndContinues"`
	ResolvesOneAddressPerConfiguredEntry   bool              `json:"resolvesOneAddressPerConfiguredEntry"`
	NewNodeUsesZeroID                      bool              `json:"newNodeUsesZeroId"`
	NewNodeDefaultTimeIsZero               bool              `json:"newNodeDefaultTimeIsZero"`
	NewNodeDefaultDropped                  bool              `json:"newNodeDefaultDropped"`
	NewNodeDefaultSampleCandidate          bool              `json:"newNodeDefaultSampleCandidate"`
	PreservesConfiguredOrder               bool              `json:"preservesConfiguredOrder"`
	PerNodeSendCancellationAware           bool              `json:"perNodeSendCancellationAware"`
	ReadySendCancelOutcome                 string            `json:"readySendCancelOutcome"`
	FreshDelayAfterRound                   bool              `json:"freshDelayAfterRound"`
	EffectiveReseedIntervalSeconds         int               `json:"effectiveReseedIntervalSeconds"`
	ConfigDefaultReseedIntervalSeconds     int               `json:"configDefaultReseedIntervalSeconds"`
	FactoryIgnoresConfiguredReseedInterval bool              `json:"factoryIgnoresConfiguredReseedInterval"`
	DefaultBootstrapNodes                  []string          `json:"defaultBootstrapNodes"`
	DefaultScalingFactor                   int               `json:"defaultScalingFactor"`
	ProductionCapacity                     int               `json:"productionCapacity"`
	ProductionConcurrency                  int               `json:"productionConcurrency"`
	LaneSharedWithRunPing                  bool              `json:"laneSharedWithRunPing"`
	ProducerDetached                       bool              `json:"producerDetached"`
	ProducerJoined                         bool              `json:"producerJoined"`
	RuntimeAvoidsPublicDNS                 bool              `json:"runtimeAvoidsPublicDns"`
	FactoryTimerRuntimeObserved            bool              `json:"factoryTimerRuntimeObserved"`
	SourceSHA256                           map[string]string `json:"sourceSha256"`
	Evidence                               string            `json:"evidence"`
}

type crawlerBootstrapPingProducerEventLog struct {
	mutex  sync.Mutex
	events []string
}

func (l *crawlerBootstrapPingProducerEventLog) append(event string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.events = append(l.events, event)
}

func (l *crawlerBootstrapPingProducerEventLog) snapshot() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return append([]string{}, l.events...)
}

type crawlerBootstrapPingProducerLane struct {
	input   chan ktable.Node
	entered chan int
	gateAt  map[int]<-chan struct{}
	events  *crawlerBootstrapPingProducerEventLog
	mutex   sync.Mutex
	calls   int
}

func (l *crawlerBootstrapPingProducerLane) In() chan<- ktable.Node {
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

func (*crawlerBootstrapPingProducerLane) Run(context.Context, func(ktable.Node)) error {
	panic("bootstrap ping producer oracle must not run the consumer lane")
}

func (l *crawlerBootstrapPingProducerLane) callCount() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.calls
}

func TestGenerateDHTCrawlerBootstrapPingProducerParity(t *testing.T) {
	fixtures := []crawlerBootstrapPingProducerFixture{
		crawlerBootstrapPingProducerSourceFixture(t),
		runCrawlerBootstrapPingProducerOrderedNumeric(t),
		runCrawlerBootstrapPingProducerInvalidContinues(t),
		runCrawlerBootstrapPingProducerOrderedPrefix(t),
	}
	if len(fixtures) != len(crawlerBootstrapPingProducerFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerBootstrapPingProducerFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerBootstrapPingProducerFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerBootstrapPingProducerFixtureIDs[index])
		}
		if fixture.Subsystem != "dht_crawler_bootstrap_ping_producer" {
			t.Fatalf("fixture %s subsystem = %q", fixture.ID, fixture.Subsystem)
		}
	}
	if fixtures[0].Classification != "SOURCE_ONLY" ||
		fixtures[1].Classification != "RUNTIME_EXACT" ||
		fixtures[2].Classification != "RUNTIME_EXACT" ||
		fixtures[3].Classification != "RUNTIME_EXACT" {
		t.Fatal("bootstrap ping producer fixture classifications drifted")
	}
	reconcileCrawlerBootstrapPingProducerFixtures(t, fixtures)
}

func crawlerBootstrapPingProducerSourceFixture(t *testing.T) crawlerBootstrapPingProducerFixture {
	t.Helper()
	assertCrawlerBootstrapPingProducerSourceShapes(t)
	config := NewDefaultConfig()
	scaling := int(config.ScalingFactor)
	if scaling != 10 {
		t.Fatalf("default scaling factor = %d, want 10", scaling)
	}
	defaultNodes := []string{
		"router.utorrent.com:6881",
		"router.bittorrent.com:6881",
		"dht.transmissionbt.com:6881",
		"dht.aelitis.com:6881",
		"router.silotis.us:6881",
		"dht.libtorrent.org:25401",
	}
	if !slices.Equal(config.BootstrapNodes, defaultNodes) {
		t.Fatalf("default bootstrap nodes = %v, want %v", config.BootstrapNodes, defaultNodes)
	}
	if config.ReseedBootstrapNodesInterval != time.Minute {
		t.Fatalf("configured reseed interval = %s, want 1m", config.ReseedBootstrapNodesInterval)
	}
	return crawlerBootstrapPingProducerFixture{
		ID:             "production_source_factory_defaults_and_lifecycle_contract",
		Subsystem:      "dht_crawler_bootstrap_ping_producer",
		Classification: "SOURCE_ONLY",
		Oracle: crawlerBootstrapPingProducerOracle{
			Composition: "exact_production_source_factory_defaults_and_lifecycle_shapes",
			Determinism: "normalized_ast_and_whole_source_sha256",
			Resolver:    "production_net_ResolveUDPAddr_single_result_interface",
			Lane:        "production_buffered_concurrent_channel_shared_with_runPing",
			Timer:       "exact_source_zero_then_effective_factory_interval_time_After_shapes",
		},
		Input: crawlerBootstrapPingProducerInput{
			Kind: "source_contract", InitialIntervalMS: 0,
			ConfiguredReseedIntervalMS: int(config.ReseedBootstrapNodesInterval / time.Millisecond),
			EffectiveReseedIntervalMS:  int((10 * time.Minute) / time.Millisecond),
			BootstrapNodes:             append([]string{}, defaultNodes...),
			LaneCapacity:               scaling,
		},
		Expected: crawlerBootstrapPingProducerExpected{
			Deliveries:        []crawlerBootstrapPingProducerDelivery{},
			ResolutionSkipped: []string{},
			Abandoned:         []string{},
			Warnings:          []string{},
			Events:            []string{},
			Source: &crawlerBootstrapPingProducerSource{
				InitialIntervalMS: 0, InitialTimerCancellationAware: true,
				ReadyInitialTimerCancelOutcome: "go_select_chooses_nondeterministically_when_zero_timer_and_cancel_are_both_ready",
				ResolvesSequentially:           true, ResolverNetwork: "udp",
				ResolutionCancellationAware: false, ResolutionErrorWarnsAndContinues: true,
				ResolvesOneAddressPerConfiguredEntry: true, NewNodeUsesZeroID: true,
				NewNodeDefaultTimeIsZero: true, NewNodeDefaultDropped: false,
				NewNodeDefaultSampleCandidate: true, PreservesConfiguredOrder: true,
				PerNodeSendCancellationAware: true,
				ReadySendCancelOutcome:       "go_select_chooses_nondeterministically_when_both_are_ready",
				FreshDelayAfterRound:         true, EffectiveReseedIntervalSeconds: 600,
				ConfigDefaultReseedIntervalSeconds:     60,
				FactoryIgnoresConfiguredReseedInterval: true,
				DefaultBootstrapNodes:                  append([]string{}, defaultNodes...),
				DefaultScalingFactor:                   scaling, ProductionCapacity: scaling,
				ProductionConcurrency: scaling, LaneSharedWithRunPing: true,
				ProducerDetached: true, ProducerJoined: false,
				RuntimeAvoidsPublicDNS: true, FactoryTimerRuntimeObserved: false,
				SourceSHA256: crawlerBootstrapPingProducerSourceDigests(t),
				Evidence:     "actual method rows use numeric literals plus one locally rejected malformed literal and return before the effective ten-minute delay; public DNS, the factory timer, synchronous resolver cancellation, and equal-ready Go select outcomes remain source evidence",
			},
		},
	}
}

func runCrawlerBootstrapPingProducerOrderedNumeric(t *testing.T) crawlerBootstrapPingProducerFixture {
	t.Helper()
	configured := []string{"192.0.2.10:6881", "[2001:db8::10]:6882"}
	events := &crawlerBootstrapPingProducerEventLog{}
	lane := &crawlerBootstrapPingProducerLane{
		input: make(chan ktable.Node, 2), entered: make(chan int, 4),
		gateAt: map[int]<-chan struct{}{}, events: events,
	}
	logger, logs := crawlerBootstrapPingProducerLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := crawler{
		bootstrapNodes: configured, reseedBootstrapNodesInterval: time.Hour,
		nodesForPing: lane, logger: logger,
	}
	done := make(chan struct{})
	go func() {
		events.append("run_start")
		c.reseedBootstrapNodes(ctx)
		events.append("return")
		close(done)
	}()
	deliveries := make([]crawlerBootstrapPingProducerDelivery, 0, len(configured))
	for index, address := range configured {
		crawlerBootstrapPingProducerWaitForLaneCall(t, lane, index+1)
		select {
		case node := <-lane.input:
			deliveries = append(deliveries, crawlerBootstrapPingProducerProject(address, node))
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for delivery %d", index+1)
		}
	}
	events.append("cancel_before_second_round")
	cancel()
	crawlerBootstrapPingProducerWaitDone(t, done)
	if lane.callCount() != 2 {
		t.Fatalf("ordered lane calls = %d, want 2", lane.callCount())
	}
	if logs.Len() != 0 {
		t.Fatalf("ordered warnings = %v, want none", crawlerBootstrapPingProducerWarnings(logs))
	}
	return crawlerBootstrapPingProducerFixture{
		ID:        "ordered_numeric_ipv4_ipv6_delivery_then_cancel_before_second_round",
		Subsystem: "dht_crawler_bootstrap_ping_producer", Classification: "RUNTIME_EXACT",
		Oracle: crawlerBootstrapPingProducerRuntimeOracle(
			"numeric_ipv4_ipv6_resolution_and_controller_cancellation_after_committed_round"),
		Input: crawlerBootstrapPingProducerInput{
			Kind: "actual_reseedBootstrapNodes", EffectiveReseedIntervalMS: int(time.Hour / time.Millisecond),
			BootstrapNodes: append([]string{}, configured...), LaneCapacity: 2,
		},
		Expected: crawlerBootstrapPingProducerExpected{
			LaneInCalls: 2, Deliveries: deliveries, ResolutionSkipped: []string{},
			Abandoned: []string{}, Warnings: []string{}, Events: events.snapshot(),
			RunReturned: true, ContextCancelled: true,
		},
	}
}

func runCrawlerBootstrapPingProducerInvalidContinues(t *testing.T) crawlerBootstrapPingProducerFixture {
	t.Helper()
	configured := []string{"not-an-address", "192.0.2.11:6883"}
	events := &crawlerBootstrapPingProducerEventLog{}
	lane := &crawlerBootstrapPingProducerLane{
		input: make(chan ktable.Node, 1), entered: make(chan int, 2),
		gateAt: map[int]<-chan struct{}{}, events: events,
	}
	logger, logs := crawlerBootstrapPingProducerLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := crawler{
		bootstrapNodes: configured, reseedBootstrapNodesInterval: time.Hour,
		nodesForPing: lane, logger: logger,
	}
	done := make(chan struct{})
	go func() {
		events.append("run_start")
		c.reseedBootstrapNodes(ctx)
		events.append("return")
		close(done)
	}()
	crawlerBootstrapPingProducerWaitForLaneCall(t, lane, 1)
	var delivery crawlerBootstrapPingProducerDelivery
	select {
	case node := <-lane.input:
		delivery = crawlerBootstrapPingProducerProject(configured[1], node)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for valid delivery after malformed address")
	}
	events.append("cancel_before_second_round")
	cancel()
	crawlerBootstrapPingProducerWaitDone(t, done)
	warnings := crawlerBootstrapPingProducerWarnings(logs)
	if !slices.Equal(warnings, []string{"failed_to_resolve_bootstrap_node_address"}) {
		t.Fatalf("warnings = %v, want one resolution warning", warnings)
	}
	if lane.callCount() != 1 {
		t.Fatalf("invalid-continuation lane calls = %d, want 1", lane.callCount())
	}
	return crawlerBootstrapPingProducerFixture{
		ID:        "malformed_address_warns_and_continues_to_later_valid_address",
		Subsystem: "dht_crawler_bootstrap_ping_producer", Classification: "RUNTIME_EXACT",
		Oracle: crawlerBootstrapPingProducerRuntimeOracle(
			"locally_rejected_malformed_literal_warning_then_later_numeric_delivery"),
		Input: crawlerBootstrapPingProducerInput{
			Kind: "actual_reseedBootstrapNodes", EffectiveReseedIntervalMS: int(time.Hour / time.Millisecond),
			BootstrapNodes: append([]string{}, configured...), LaneCapacity: 1,
		},
		Expected: crawlerBootstrapPingProducerExpected{
			LaneInCalls: 1, Deliveries: []crawlerBootstrapPingProducerDelivery{delivery},
			ResolutionSkipped: []string{configured[0]}, Abandoned: []string{},
			Warnings: warnings, Events: events.snapshot(), RunReturned: true, ContextCancelled: true,
		},
	}
}

func runCrawlerBootstrapPingProducerOrderedPrefix(t *testing.T) crawlerBootstrapPingProducerFixture {
	t.Helper()
	configured := []string{
		"192.0.2.21:6891", "192.0.2.22:6892", "192.0.2.23:6893", "192.0.2.24:6894",
	}
	events := &crawlerBootstrapPingProducerEventLog{}
	thirdGate := make(chan struct{})
	var releaseOnce sync.Once
	releaseThird := func() { releaseOnce.Do(func() { close(thirdGate) }) }
	defer releaseThird()
	lane := &crawlerBootstrapPingProducerLane{
		input: make(chan ktable.Node, 2), entered: make(chan int, 8),
		gateAt: map[int]<-chan struct{}{3: thirdGate}, events: events,
	}
	logger, logs := crawlerBootstrapPingProducerLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := crawler{
		bootstrapNodes: configured, reseedBootstrapNodesInterval: time.Hour,
		nodesForPing: lane, logger: logger,
	}
	done := make(chan struct{})
	go func() {
		events.append("run_start")
		c.reseedBootstrapNodes(ctx)
		events.append("return")
		close(done)
	}()
	for want := 1; want <= 3; want++ {
		crawlerBootstrapPingProducerWaitForLaneCall(t, lane, want)
	}
	if len(lane.input) != 2 {
		t.Fatalf("queued prefix = %d, want 2", len(lane.input))
	}
	events.append("cancel")
	cancel()
	releaseThird()
	crawlerBootstrapPingProducerWaitDone(t, done)
	deliveries := make([]crawlerBootstrapPingProducerDelivery, 0, 2)
	for index := 0; index < 2; index++ {
		deliveries = append(deliveries,
			crawlerBootstrapPingProducerProject(configured[index], <-lane.input))
	}
	if lane.callCount() != 3 {
		t.Fatalf("ordered-prefix lane calls = %d, want 3", lane.callCount())
	}
	if logs.Len() != 0 {
		t.Fatalf("ordered-prefix warnings = %v, want none", crawlerBootstrapPingProducerWarnings(logs))
	}
	return crawlerBootstrapPingProducerFixture{
		ID:        "ordered_prefix_then_cancel_at_blocked_third_ping_send",
		Subsystem: "dht_crawler_bootstrap_ping_producer", Classification: "RUNTIME_EXACT",
		Oracle: crawlerBootstrapPingProducerRuntimeOracle(
			"capacity_two_ping_lane_with_third_In_gate_and_numeric_resolution"),
		Input: crawlerBootstrapPingProducerInput{
			Kind: "actual_reseedBootstrapNodes", EffectiveReseedIntervalMS: int(time.Hour / time.Millisecond),
			BootstrapNodes: append([]string{}, configured...), LaneCapacity: 2, CancelAtLaneInCall: 3,
		},
		Expected: crawlerBootstrapPingProducerExpected{
			LaneInCalls: 3, Deliveries: deliveries, ResolutionSkipped: []string{},
			Abandoned: append([]string{}, configured[2:]...), Warnings: []string{},
			Events: events.snapshot(), RunReturned: true, ContextCancelled: true,
		},
	}
}

func crawlerBootstrapPingProducerRuntimeOracle(determinism string) crawlerBootstrapPingProducerOracle {
	return crawlerBootstrapPingProducerOracle{
		Composition: "actual_crawler_reseedBootstrapNodes_with_production_numeric_resolver_observer_logger_and_manual_lane",
		Determinism: determinism,
		Resolver:    "production_net_ResolveUDPAddr_with_numeric_or_locally_rejected_literals_only",
		Lane:        "manual_capacity_controlled_lane_implementing_BufferedConcurrentChannel_contract",
		Timer:       "production_initial_zero_time_After_then_cancel_before_positive_reseed_timer",
	}
}

func crawlerBootstrapPingProducerProject(
	configured string,
	node ktable.Node,
) crawlerBootstrapPingProducerDelivery {
	return crawlerBootstrapPingProducerDelivery{
		Configured: configured, ID: node.ID().String(), Addr: node.Addr().String(),
		TimeIsZero: node.Time().IsZero(), Dropped: node.Dropped(),
		SampleInfoHashesCandidate: node.IsSampleInfoHashesCandidate(),
	}
}

func crawlerBootstrapPingProducerLogger() (*zap.SugaredLogger, *observer.ObservedLogs) {
	core, logs := observer.New(zap.WarnLevel)
	return zap.New(core).Sugar(), logs
}

func crawlerBootstrapPingProducerWarnings(logs *observer.ObservedLogs) []string {
	entries := logs.All()
	warnings := make([]string, 0, len(entries))
	for _, entry := range entries {
		if strings.HasPrefix(entry.Message, "failed to resolve bootstrap node address:") {
			warnings = append(warnings, "failed_to_resolve_bootstrap_node_address")
			continue
		}
		warnings = append(warnings, entry.Message)
	}
	return warnings
}

func crawlerBootstrapPingProducerWaitForLaneCall(
	t *testing.T,
	lane *crawlerBootstrapPingProducerLane,
	want int,
) {
	t.Helper()
	select {
	case got := <-lane.entered:
		if got != want {
			t.Fatalf("lane In call = %d, want %d", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for lane In call %d", want)
	}
}

func crawlerBootstrapPingProducerWaitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("bootstrap ping producer did not return after cancellation")
	}
}

func assertCrawlerBootstrapPingProducerSourceShapes(t *testing.T) {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	producerSet, producer := crawlerPingWorkerParseFunc(
		t, filepath.Join(root, "internal/dhtcrawler/bootstrap.go"), "reseedBootstrapNodes")
	wantSet, want := crawlerFindNodeWorkerParseSourceFunc(t, `package dhtcrawler
func (c *crawler) reseedBootstrapNodes(ctx context.Context) {
	interval := time.Duration(0)
	for {
		select {
		case <-ctx.Done(): return
		case <-time.After(interval):
			for _, strAddr := range c.bootstrapNodes {
				addr, err := net.ResolveUDPAddr("udp", strAddr)
				if err != nil {
					c.logger.Warnf("failed to resolve bootstrap node address: %s", err)
					continue
				}
				select {
				case <-ctx.Done(): return
				case c.nodesForPing.In() <- ktable.NewNode(ktable.ID{}, addr.AddrPort()): continue
				}
			}
		}
		interval = c.reseedBootstrapNodesInterval
	}
}`)
	crawlerFindNodeWorkerAssertBody(t, producerSet, producer, wantSet, want,
		"reseedBootstrapNodes")

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
	crawlerPingWorkerAssertExpr(t, factorySet, values["bootstrapNodes"], "params.Config.BootstrapNodes")
	crawlerPingWorkerAssertExpr(t, factorySet, values["reseedBootstrapNodesInterval"], "time.Minute * 10")
	crawlerPingWorkerAssertExpr(t, factorySet, values["nodesForPing"],
		"concurrency.NewBufferedConcurrentChannel[ktable.Node](scalingFactor, scalingFactor)")
	factoryText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, factorySet, factory.Body))
	for _, required := range []string{"go c.start()", "close(c.stopped)"} {
		if !strings.Contains(factoryText, crawlerPingWorkerTokenText(required)) {
			t.Fatalf("crawler factory missing %s", required)
		}
	}

	configSet, config := crawlerPingWorkerParseFunc(
		t, filepath.Join(root, "internal/dhtcrawler/config.go"), "NewDefaultConfig")
	configValues := make(map[string]ast.Expr)
	ast.Inspect(config.Body, func(node ast.Node) bool {
		entry, ok := node.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		if key, ok := entry.Key.(*ast.Ident); ok {
			configValues[key.Name] = entry.Value
		}
		return true
	})
	crawlerPingWorkerAssertExpr(t, configSet, configValues["ScalingFactor"], "10")
	crawlerPingWorkerAssertExpr(t, configSet, configValues["BootstrapNodes"], "defaultBootstrapNodes")
	crawlerPingWorkerAssertExpr(t, configSet, configValues["ReseedBootstrapNodesInterval"], "time.Minute")

	crawlerSet, start := crawlerPingWorkerParseFunc(
		t, filepath.Join(root, "internal/dhtcrawler/crawler.go"), "start")
	startText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, crawlerSet, start.Body))
	for _, required := range []string{
		"ctx, cancel := context.WithCancel(context.Background())", "defer cancel()",
		"go c.runPing(ctx)", "go c.reseedBootstrapNodes(ctx)", "<-c.stopped",
	} {
		if !strings.Contains(startText, crawlerPingWorkerTokenText(required)) {
			t.Fatalf("crawler start missing %s", required)
		}
	}

	runPingSet, runPing := crawlerPingWorkerParseFunc(
		t, filepath.Join(root, "internal/dhtcrawler/ping.go"), "runPing")
	runPingText := crawlerPingWorkerASTText(t, runPingSet, runPing.Body)
	if !strings.Contains(runPingText, "c.nodesForPing.Run(ctx") {
		t.Fatal("ping consumer no longer uses the bootstrap producer's shared lane")
	}

	channelSet, in := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/concurrency/buffered_concurrent_channel.go"), "In")
	wantChannelSet, wantIn := crawlerFindNodeWorkerParseSourceFunc(t, `package concurrency
func (ch bufferedConcurrentChannel[T]) In() chan<- T { return ch.ch }`)
	crawlerFindNodeWorkerAssertBody(t, channelSet, in, wantChannelSet, wantIn,
		"BufferedConcurrentChannel.In")

	assertCrawlerOldNodePingProducerMethodBody(t,
		filepath.Join(root, "internal/protocol/dht/ktable/node.go"), "nodeBase", "Time",
		`package ktable
func (nodeBase) Time() time.Time { return time.Time{} }`)
	assertCrawlerOldNodePingProducerMethodBody(t,
		filepath.Join(root, "internal/protocol/dht/ktable/node.go"), "nodeBase", "Dropped",
		`package ktable
func (nodeBase) Dropped() bool { return false }`)
	assertCrawlerOldNodePingProducerMethodBody(t,
		filepath.Join(root, "internal/protocol/dht/ktable/node.go"), "nodeBase", "IsSampleInfoHashesCandidate",
		`package ktable
func (nodeBase) IsSampleInfoHashesCandidate() bool { return true }`)
}

func crawlerBootstrapPingProducerSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	paths := []string{
		"internal/concurrency/buffered_concurrent_channel.go",
		"internal/dhtcrawler/bootstrap.go",
		"internal/dhtcrawler/config.go",
		"internal/dhtcrawler/crawler.go",
		"internal/dhtcrawler/factory.go",
		"internal/protocol/dht/ktable/node.go",
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

func reconcileCrawlerBootstrapPingProducerFixtures(
	t *testing.T,
	fixtures []crawlerBootstrapPingProducerFixture,
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
	if crawlerBootstrapPingProducerFixtureSHA256 != "" &&
		actualHash != crawlerBootstrapPingProducerFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash,
			crawlerBootstrapPingProducerFixtureSHA256)
	}
	path := filepath.Join(crawlerPingWorkerRoot(t),
		"testdata/parity/dht/dht_crawler_bootstrap_ping_producer.jsonl")
	if *updateDHTCrawlerBootstrapPingProducerParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-bootstrap-ping-producer-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler bootstrap-ping-producer fixture is stale; rerun with -update-dht-crawler-bootstrap-ping-producer-parity")
	}
}

var _ concurrency.BufferedConcurrentChannel[ktable.Node] = (*crawlerBootstrapPingProducerLane)(nil)
