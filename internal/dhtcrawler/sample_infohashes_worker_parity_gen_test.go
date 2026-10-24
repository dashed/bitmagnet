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
	"hash/fnv"
	"math"
	"math/rand"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	boom "github.com/tylertreat/BoomFilters"
)

var updateDHTCrawlerSampleInfoHashesWorkerParity = flag.Bool(
	"update-dht-crawler-sample-infohashes-worker-parity",
	false,
	"rewrite the Rust DHT crawler sample-infohashes-worker parity fixture",
)

const crawlerSampleInfoHashesWorkerFixtureSHA256 = "8533c4644ceaed71a372ef52ec944f1b625f48c0042e1ef7f45990dbe0ef2744"

var crawlerSampleInfoHashesWorkerFixtureIDs = [...]string{
	"production_source_callback_interval_put_and_fanout_contract",
	"actual_buffered_lane_mutated_interface_node_candidate_skipped",
	"eligible_client_error_drops_advertised_node",
	"ordered_novel_prefix_cancel_after_full_dedupe",
	"clamp_put_then_detached_recursive_prefix_cancel",
}

type crawlerSampleInfoHashesWorkerFixture struct {
	ID             string                                `json:"id"`
	Subsystem      string                                `json:"subsystem"`
	Classification string                                `json:"classification"`
	Oracle         crawlerSampleInfoHashesWorkerOracle   `json:"oracle"`
	Input          crawlerSampleInfoHashesWorkerInput    `json:"input"`
	Expected       crawlerSampleInfoHashesWorkerExpected `json:"expected"`
}

type crawlerSampleInfoHashesWorkerOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Lane        string `json:"lane"`
	Client      string `json:"client"`
	Deduper     string `json:"deduper"`
	Table       string `json:"table"`
	Triage      string `json:"triage"`
	Fanout      string `json:"fanout"`
	Clock       string `json:"clock"`
}

type crawlerSampleInfoHashesWorkerInput struct {
	Kind                     string                                 `json:"kind"`
	LaneCapacity             int                                    `json:"laneCapacity"`
	LaneConcurrency          int                                    `json:"laneConcurrency"`
	Node                     *crawlerSampleInfoHashesWorkerNode     `json:"node,omitempty"`
	Response                 *crawlerSampleInfoHashesWorkerResponse `json:"response,omitempty"`
	SoughtTarget             string                                 `json:"soughtTarget,omitempty"`
	PreloadedHashes          []string                               `json:"preloadedHashes,omitempty"`
	OracleRNGSeed            int64                                  `json:"oracleRngSeed,omitempty"`
	HashIndexes              map[string][]uint                      `json:"hashIndexes,omitempty"`
	MutateCandidateAfterTake bool                                   `json:"mutateCandidateAfterTake,omitempty"`
	TriageCapacity           int                                    `json:"triageCapacity"`
	CancelAtTriageInCall     int                                    `json:"cancelAtTriageInCall"`
	DiscoveryCapacity        int                                    `json:"discoveryCapacity"`
	CancelAtDiscoveryInCall  int                                    `json:"cancelAtDiscoveryInCall"`
}

type crawlerSampleInfoHashesWorkerNode struct {
	Token            string   `json:"token"`
	ID               string   `json:"id"`
	Addr             string   `json:"addr"`
	AddrReturns      []string `json:"addrReturns,omitempty"`
	InitialCandidate bool     `json:"initialCandidate"`
	FinalCandidate   bool     `json:"finalCandidate"`
}

type crawlerSampleInfoHashesWorkerResponse struct {
	Kind       string                              `json:"kind"`
	ResponseID string                              `json:"responseId"`
	Samples    []string                            `json:"samples"`
	Nodes      []crawlerSampleInfoHashesWorkerNode `json:"nodes"`
	Num        int                                 `json:"num"`
	Interval   int                                 `json:"interval"`
}

type crawlerSampleInfoHashesWorkerExpected struct {
	NodeCalls                     crawlerSampleInfoHashesWorkerNodeCalls    `json:"nodeCalls"`
	ClientCalls                   []crawlerSampleInfoHashesWorkerClientCall `json:"clientCalls"`
	SameContext                   bool                                      `json:"sameContext"`
	SourceDerivedDeduperCallOrder []string                                  `json:"sourceDerivedDeduperCallOrder"`
	DeduperPostMembership         map[string]bool                           `json:"deduperPostMembership"`
	TriageInCalls                 int                                       `json:"triageInCalls"`
	TriageDeliveries              []crawlerSampleInfoHashesWorkerTriage     `json:"triageDeliveries"`
	Commands                      []crawlerSampleInfoHashesWorkerCommand    `json:"commands"`
	DiscoveryInCalls              int                                       `json:"discoveryInCalls"`
	Discoveries                   []crawlerSampleInfoHashesWorkerNode       `json:"discoveries"`
	Events                        []string                                  `json:"events"`
	RunReturned                   bool                                      `json:"runReturned"`
	ContextCancelled              bool                                      `json:"contextCancelled"`
	CallbackCompletionObserved    bool                                      `json:"callbackCompletionObserved"`
	FanoutCompletionObserved      bool                                      `json:"fanoutCompletionObserved"`
	Source                        *crawlerSampleInfoHashesWorkerSource      `json:"source,omitempty"`
}

type crawlerSampleInfoHashesWorkerNodeCalls struct {
	ID                        int `json:"id"`
	Addr                      int `json:"addr"`
	Time                      int `json:"time"`
	Dropped                   int `json:"dropped"`
	SampleInfoHashesCandidate int `json:"sampleInfohashesCandidate"`
}

type crawlerSampleInfoHashesWorkerClientCall struct {
	Addr   string `json:"addr"`
	Target string `json:"target"`
}

type crawlerSampleInfoHashesWorkerTriage struct {
	InfoHash string `json:"infoHash"`
	Node     string `json:"node"`
}

type crawlerSampleInfoHashesWorkerCommand struct {
	Kind                   string `json:"kind"`
	ID                     string `json:"id"`
	Addr                   string `json:"addr,omitempty"`
	OptionCount            int    `json:"optionCount"`
	Reason                 string `json:"reason,omitempty"`
	ErrorIdentityPreserved bool   `json:"errorIdentityPreserved"`
	StoredResponded        bool   `json:"storedResponded"`
	StoredCandidate        bool   `json:"storedCandidate"`
}

type crawlerSampleInfoHashesWorkerIntervalCase struct {
	Name              string `json:"name"`
	RawInterval       int64  `json:"rawInterval"`
	NovelCount        int    `json:"novelCount"`
	EffectiveInterval int64  `json:"effectiveInterval"`
	DurationNS        int64  `json:"durationNs"`
}

type crawlerSampleInfoHashesWorkerSource struct {
	RunErrorIgnored                                bool                                        `json:"runErrorIgnored"`
	SharedCallbackContext                          bool                                        `json:"sharedCallbackContext"`
	CandidateCheckedAtCallbackTime                 bool                                        `json:"candidateCheckedAtCallbackTime"`
	CandidateCheckedBeforeClient                   bool                                        `json:"candidateCheckedBeforeClient"`
	TargetReadAtClientCall                         bool                                        `json:"targetReadAtClientCall"`
	ResponseIDIgnored                              bool                                        `json:"responseIdIgnored"`
	ErrorDropsAdvertisedID                         bool                                        `json:"errorDropsAdvertisedId"`
	ErrorReasonWrapsCause                          bool                                        `json:"errorReasonWrapsCause"`
	SamplesProcessedInResponseOrder                bool                                        `json:"samplesProcessedInResponseOrder"`
	DeduperCalledForEverySample                    bool                                        `json:"deduperCalledForEverySample"`
	DeduperCompletesBeforeTriage                   bool                                        `json:"deduperCompletesBeforeTriage"`
	OnlyNovelHashesTriaged                         bool                                        `json:"onlyNovelHashesTriaged"`
	NodeAddressRereadPerNovelHash                  bool                                        `json:"nodeAddressRereadPerNovelHash"`
	TriageBlocksInOrder                            bool                                        `json:"triageBlocksInOrder"`
	TriageCancellationAware                        bool                                        `json:"triageCancellationAware"`
	TriageCancellationBranchReturnsBeforePutFanout bool                                        `json:"triageCancellationBranchReturnsBeforePutFanout"`
	ClampRequiresNovelAndOver300                   bool                                        `json:"clampRequiresNovelAndOver300"`
	ClampIntervalSeconds                           int                                         `json:"clampIntervalSeconds"`
	DurationConversion                             string                                      `json:"durationConversion"`
	GoIntBits                                      int                                         `json:"goIntBits"`
	IntervalCases                                  []crawlerSampleInfoHashesWorkerIntervalCase `json:"intervalCases"`
	PutUsesAdvertisedIDAndCurrentAddr              bool                                        `json:"putUsesAdvertisedIdAndCurrentAddr"`
	PutOptionOrder                                 []string                                    `json:"putOptionOrder"`
	PutDiscoveredCount                             string                                      `json:"putDiscoveredCount"`
	PutTotalCount                                  string                                      `json:"putTotalCount"`
	PutDeadlineExpression                          string                                      `json:"putDeadlineExpression"`
	PutOccursAfterAllTriage                        bool                                        `json:"putOccursAfterAllTriage"`
	PutPrecedesFanoutLaunch                        bool                                        `json:"putPrecedesFanoutLaunch"`
	FanoutUsesResponseOrder                        bool                                        `json:"fanoutUsesResponseOrder"`
	FanoutReadsCapturedResponseInGoroutine         bool                                        `json:"fanoutReadsCapturedResponseInGoroutine"`
	FanoutDeepCopiesResponseNodes                  bool                                        `json:"fanoutDeepCopiesResponseNodes"`
	FanoutDetached                                 bool                                        `json:"fanoutDetached"`
	FanoutJoined                                   bool                                        `json:"fanoutJoined"`
	FanoutWholeListTimeoutMS                       int                                         `json:"fanoutWholeListTimeoutMs"`
	FanoutCancellationAware                        bool                                        `json:"fanoutCancellationAware"`
	ProductionCapacity                             int                                         `json:"productionCapacity"`
	ProductionConcurrency                          int                                         `json:"productionConcurrency"`
	DefaultScalingFactor                           int                                         `json:"defaultScalingFactor"`
	ConsumerDequeuesBeforeSemaphore                bool                                        `json:"consumerDequeuesBeforeSemaphore"`
	AcquireCancellationDropsDequeuedItem           bool                                        `json:"acquireCancellationDropsDequeuedItem"`
	MaximumRetainedWork                            string                                      `json:"maximumRetainedWork"`
	ConsumerCallbacksDetached                      bool                                        `json:"consumerCallbacksDetached"`
	ConsumerCallbacksJoined                        bool                                        `json:"consumerCallbacksJoined"`
	ClosedInputChecksOpenBoolean                   bool                                        `json:"closedInputChecksOpenBoolean"`
	ClosedInputOutcome                             string                                      `json:"closedInputOutcome"`
	ProductionTriageCapacity                       int                                         `json:"productionTriageCapacity"`
	ProductionTriageMaxBatchSize                   int                                         `json:"productionTriageMaxBatchSize"`
	ProductionTriageIntervalMS                     int                                         `json:"productionTriageIntervalMs"`
	ProductionTriageOutputCapacity                 int                                         `json:"productionTriageOutputCapacity"`
	ProductionDiscoveryCapacity                    int                                         `json:"productionDiscoveryCapacity"`
	ProductionDiscoveryMaxBatchSize                int                                         `json:"productionDiscoveryMaxBatchSize"`
	ProductionDiscoveryIntervalMS                  int                                         `json:"productionDiscoveryIntervalMs"`
	ProductionDiscoveryOutputCapacity              int                                         `json:"productionDiscoveryOutputCapacity"`
	StartLaunchesWorkerDetached                    bool                                        `json:"startLaunchesWorkerDetached"`
	StartWaitsOnlyStopped                          bool                                        `json:"startWaitsOnlyStopped"`
	StartDefersSharedContextCancel                 bool                                        `json:"startDefersSharedContextCancel"`
	StartJoinsWorkerCallbacksOrFanout              bool                                        `json:"startJoinsWorkerCallbacksOrFanout"`
	FanoutCanOutliveWorkerPermit                   bool                                        `json:"fanoutCanOutliveWorkerPermit"`
	PrerequisiteFixtureSHA256                      map[string]string                           `json:"prerequisiteFixtureSha256"`
	EvidenceCommit                                 map[string]string                           `json:"evidenceCommit"`
	SourceSHA256                                   map[string]string                           `json:"sourceSha256"`
	Nonclaims                                      []string                                    `json:"nonclaims"`
	Evidence                                       string                                      `json:"evidence"`
}

type crawlerSampleInfoHashesWorkerEventLog struct {
	mutex  sync.Mutex
	events []string
}

func (l *crawlerSampleInfoHashesWorkerEventLog) append(event string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.events = append(l.events, event)
}

func (l *crawlerSampleInfoHashesWorkerEventLog) snapshot() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return append([]string{}, l.events...)
}

type crawlerSampleInfoHashesWorkerProbeNode struct {
	mutex             sync.Mutex
	token             string
	id                protocol.ID
	addr              netip.AddrPort
	addrs             []netip.AddrPort
	initialCandidate  bool
	candidate         bool
	candidateGate     <-chan struct{}
	candidateEntered  chan<- struct{}
	candidateReturned chan<- struct{}
	events            *crawlerSampleInfoHashesWorkerEventLog
	calls             crawlerSampleInfoHashesWorkerNodeCalls
}

func (n *crawlerSampleInfoHashesWorkerProbeNode) ID() protocol.ID {
	n.mutex.Lock()
	n.calls.ID++
	id := n.id
	n.mutex.Unlock()
	n.events.append("node_id:" + n.token)
	return id
}

func (n *crawlerSampleInfoHashesWorkerProbeNode) Addr() netip.AddrPort {
	n.mutex.Lock()
	n.calls.Addr++
	index := n.calls.Addr - 1
	addr := n.addrAt(index)
	n.mutex.Unlock()
	n.events.append("node_addr:" + n.token)
	return addr
}

func (n *crawlerSampleInfoHashesWorkerProbeNode) addrAt(index int) netip.AddrPort {
	if index < len(n.addrs) {
		return n.addrs[index]
	}
	if len(n.addrs) > 0 {
		return n.addrs[len(n.addrs)-1]
	}
	return n.addr
}

func (n *crawlerSampleInfoHashesWorkerProbeNode) Time() time.Time {
	n.mutex.Lock()
	n.calls.Time++
	n.mutex.Unlock()
	n.events.append("node_time:" + n.token)
	return time.Time{}
}

func (n *crawlerSampleInfoHashesWorkerProbeNode) Dropped() bool {
	n.mutex.Lock()
	n.calls.Dropped++
	n.mutex.Unlock()
	n.events.append("node_dropped:" + n.token)
	return false
}

func (n *crawlerSampleInfoHashesWorkerProbeNode) IsSampleInfoHashesCandidate() bool {
	n.mutex.Lock()
	n.calls.SampleInfoHashesCandidate++
	gate := n.candidateGate
	entered := n.candidateEntered
	returned := n.candidateReturned
	n.candidateEntered = nil
	n.candidateReturned = nil
	n.mutex.Unlock()
	n.events.append("node_candidate_enter:" + n.token)
	if entered != nil {
		entered <- struct{}{}
	}
	if gate != nil {
		<-gate
	}
	n.mutex.Lock()
	candidate := n.candidate
	n.mutex.Unlock()
	n.events.append(fmt.Sprintf("node_candidate_return:%s:%t", n.token, candidate))
	if returned != nil {
		returned <- struct{}{}
	}
	return candidate
}

func (n *crawlerSampleInfoHashesWorkerProbeNode) setCandidate(candidate bool) {
	n.mutex.Lock()
	n.candidate = candidate
	n.mutex.Unlock()
	n.events.append(fmt.Sprintf("node_candidate_mutated:%s:%t", n.token, candidate))
}

func (n *crawlerSampleInfoHashesWorkerProbeNode) snapshotCalls() crawlerSampleInfoHashesWorkerNodeCalls {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	return n.calls
}

func (n *crawlerSampleInfoHashesWorkerProbeNode) fixtureNode() crawlerSampleInfoHashesWorkerNode {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	addrReturns := make([]string, 0, n.calls.Addr)
	for index := range n.calls.Addr {
		addrReturns = append(addrReturns, n.addrAt(index).String())
	}
	return crawlerSampleInfoHashesWorkerNode{
		Token: n.token, ID: n.id.String(), Addr: n.addr.String(), AddrReturns: addrReturns,
		InitialCandidate: n.initialCandidate, FinalCandidate: n.candidate,
	}
}

type crawlerSampleInfoHashesWorkerManualLane struct {
	nodes     []ktable.Node
	events    *crawlerSampleInfoHashesWorkerEventLog
	completed bool
}

func (*crawlerSampleInfoHashesWorkerManualLane) In() chan<- ktable.Node {
	panic("sample-infohashes worker oracle must not request the manual lane sender")
}

func (l *crawlerSampleInfoHashesWorkerManualLane) Run(_ context.Context, callback func(ktable.Node)) error {
	for index, node := range l.nodes {
		l.events.append(fmt.Sprintf("callback_begin:%d", index))
		callback(node)
		l.events.append(fmt.Sprintf("callback_complete:%d", index))
	}
	l.completed = true
	return nil
}

type crawlerSampleInfoHashesWorkerGatedBatching[T any] struct {
	input    chan T
	entered  chan int
	preGates map[int]<-chan struct{}
	gates    map[int]<-chan struct{}
	events   *crawlerSampleInfoHashesWorkerEventLog
	label    string
	mutex    sync.Mutex
	calls    int
}

func (l *crawlerSampleInfoHashesWorkerGatedBatching[T]) In() chan<- T {
	l.mutex.Lock()
	l.calls++
	call := l.calls
	l.mutex.Unlock()
	if gate := l.preGates[call]; gate != nil {
		<-gate
	}
	l.events.append(fmt.Sprintf("%s_in:%d", l.label, call))
	if l.entered != nil {
		l.entered <- call
	}
	if gate := l.gates[call]; gate != nil {
		<-gate
	}
	return l.input
}

func (*crawlerSampleInfoHashesWorkerGatedBatching[T]) Out() <-chan []T {
	panic("sample-infohashes worker oracle must not request batching output")
}

func (l *crawlerSampleInfoHashesWorkerGatedBatching[T]) callCount() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.calls
}

type crawlerSampleInfoHashesWorkerClient struct {
	client.Client
	wantContext context.Context
	response    crawlerSampleInfoHashesWorkerResponse
	calls       []crawlerSampleInfoHashesWorkerClientCall
	sameContext bool
	events      *crawlerSampleInfoHashesWorkerEventLog
}

func (c *crawlerSampleInfoHashesWorkerClient) SampleInfoHashes(
	ctx context.Context,
	addr netip.AddrPort,
	target protocol.ID,
) (client.SampleInfoHashesResult, error) {
	c.sameContext = c.sameContext && ctx == c.wantContext
	c.calls = append(c.calls, crawlerSampleInfoHashesWorkerClientCall{
		Addr: addr.String(), Target: target.String(),
	})
	c.events.append("client_sample_infohashes")
	result := client.SampleInfoHashesResult{
		ID:       protocol.MustParseID(c.response.ResponseID),
		Num:      c.response.Num,
		Interval: c.response.Interval,
	}
	for _, sample := range c.response.Samples {
		result.Samples = append(result.Samples, protocol.MustParseID(sample))
	}
	for _, node := range c.response.Nodes {
		result.Nodes = append(result.Nodes, client.NodeInfo{
			ID: protocol.MustParseID(node.ID), Addr: netip.MustParseAddrPort(node.Addr),
		})
	}
	if c.response.Kind == "error" {
		return result, crawlerSampleInfoHashesWorkerSentinel
	}
	return result, nil
}

var crawlerSampleInfoHashesWorkerSentinel = errors.New("oracle sample_infohashes failure")

type crawlerSampleInfoHashesWorkerTracingTable struct {
	ktable.Table
	events   *crawlerSampleInfoHashesWorkerEventLog
	commands []crawlerSampleInfoHashesWorkerCommand
}

func TestGenerateDHTCrawlerSampleInfoHashesWorkerParity(t *testing.T) {
	fixtures := []crawlerSampleInfoHashesWorkerFixture{
		crawlerSampleInfoHashesWorkerSourceFixture(t),
		runCrawlerSampleInfoHashesWorkerCandidateMutation(t),
		runCrawlerSampleInfoHashesWorkerError(t),
		runCrawlerSampleInfoHashesWorkerTriageCancellation(t),
		runCrawlerSampleInfoHashesWorkerFanoutCancellation(t),
	}
	if len(fixtures) != len(crawlerSampleInfoHashesWorkerFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerSampleInfoHashesWorkerFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerSampleInfoHashesWorkerFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerSampleInfoHashesWorkerFixtureIDs[index])
		}
		if fixture.Subsystem != "dht_crawler_sample_infohashes_worker" {
			t.Fatalf("fixture %s subsystem = %q", fixture.ID, fixture.Subsystem)
		}
		wantClassification := "RUNTIME_EXACT"
		if index == 0 {
			wantClassification = "SOURCE_ONLY"
		}
		if fixture.Classification != wantClassification {
			t.Fatalf("fixture %s classification = %q, want %q", fixture.ID, fixture.Classification, wantClassification)
		}
	}
	reconcileCrawlerSampleInfoHashesWorkerFixtures(t, fixtures)
}

func crawlerSampleInfoHashesWorkerSourceFixture(t *testing.T) crawlerSampleInfoHashesWorkerFixture {
	t.Helper()
	assertCrawlerSampleInfoHashesWorkerSourceShapes(t)
	if strconv.IntSize != 64 {
		t.Fatalf("sample-infohashes worker Go overflow oracle requires 64-bit int, got %d", strconv.IntSize)
	}
	config := NewDefaultConfig()
	if config.ScalingFactor != 10 {
		t.Fatalf("default scaling factor = %d, want 10", config.ScalingFactor)
	}
	intervalCases := []crawlerSampleInfoHashesWorkerIntervalCase{
		crawlerSampleInfoHashesWorkerInterval("negative_novel_unclamped", -7, 1),
		crawlerSampleInfoHashesWorkerInterval("boundary_300_novel_unclamped", 300, 1),
		crawlerSampleInfoHashesWorkerInterval("over_300_novel_clamped", 301, 1),
		crawlerSampleInfoHashesWorkerInterval("over_300_zero_novel_unclamped", 301, 0),
		crawlerSampleInfoHashesWorkerInterval("max_int_novel_clamped_before_convert", math.MaxInt64, 1),
		crawlerSampleInfoHashesWorkerInterval("max_int_zero_novel_wraps_duration", math.MaxInt64, 0),
		crawlerSampleInfoHashesWorkerInterval("min_int_novel_unclamped_wraps_duration", math.MinInt64, 1),
		crawlerSampleInfoHashesWorkerInterval("min_int_zero_novel_wraps_duration", math.MinInt64, 0),
	}
	wantIntervals := []crawlerSampleInfoHashesWorkerIntervalCase{
		{Name: "negative_novel_unclamped", RawInterval: -7, NovelCount: 1, EffectiveInterval: -7, DurationNS: -7_000_000_000},
		{Name: "boundary_300_novel_unclamped", RawInterval: 300, NovelCount: 1, EffectiveInterval: 300, DurationNS: 300_000_000_000},
		{Name: "over_300_novel_clamped", RawInterval: 301, NovelCount: 1, EffectiveInterval: 60, DurationNS: 60_000_000_000},
		{Name: "over_300_zero_novel_unclamped", RawInterval: 301, NovelCount: 0, EffectiveInterval: 301, DurationNS: 301_000_000_000},
		{Name: "max_int_novel_clamped_before_convert", RawInterval: math.MaxInt64, NovelCount: 1, EffectiveInterval: 60, DurationNS: 60_000_000_000},
		{Name: "max_int_zero_novel_wraps_duration", RawInterval: math.MaxInt64, NovelCount: 0, EffectiveInterval: math.MaxInt64, DurationNS: -1_000_000_000},
		{Name: "min_int_novel_unclamped_wraps_duration", RawInterval: math.MinInt64, NovelCount: 1, EffectiveInterval: math.MinInt64, DurationNS: 0},
		{Name: "min_int_zero_novel_wraps_duration", RawInterval: math.MinInt64, NovelCount: 0, EffectiveInterval: math.MinInt64, DurationNS: 0},
	}
	if !reflect.DeepEqual(intervalCases, wantIntervals) {
		t.Fatalf("signed interval cases = %+v, want %+v", intervalCases, wantIntervals)
	}
	return crawlerSampleInfoHashesWorkerFixture{
		ID: crawlerSampleInfoHashesWorkerFixtureIDs[0], Subsystem: "dht_crawler_sample_infohashes_worker",
		Classification: "SOURCE_ONLY",
		Oracle: crawlerSampleInfoHashesWorkerOracle{
			Composition: "exact_production_worker_factory_start_channel_and_ktable_source_contract",
			Determinism: "normalized_AST_exact_source_SHA256_prerequisite_fixture_SHA256_and_signed_integer_vectors",
			Lane:        "production_buffered_concurrent_channel_source_only",
			Client:      "production_dht_client_interface_source_only",
			Deduper:     "composes_strict_actual_ignore_hashes_oracle",
			Table:       "production_ktable_commands_and_options_source_only",
			Triage:      "production_shared_batching_channel_source_only",
			Fanout:      "production_shared_discovered_nodes_batching_channel_source_only",
			Clock:       "duration_arithmetic_only_no_wall_clock_value_claim",
		},
		Input: crawlerSampleInfoHashesWorkerInput{Kind: "source_contract"},
		Expected: crawlerSampleInfoHashesWorkerExpected{
			ClientCalls: []crawlerSampleInfoHashesWorkerClientCall{}, SourceDerivedDeduperCallOrder: []string{},
			DeduperPostMembership: map[string]bool{},
			TriageDeliveries:      []crawlerSampleInfoHashesWorkerTriage{}, Commands: []crawlerSampleInfoHashesWorkerCommand{},
			Discoveries: []crawlerSampleInfoHashesWorkerNode{}, Events: []string{}, RunReturned: true,
			Source: &crawlerSampleInfoHashesWorkerSource{
				RunErrorIgnored: true, SharedCallbackContext: true,
				CandidateCheckedAtCallbackTime: true, CandidateCheckedBeforeClient: true,
				TargetReadAtClientCall: true, ResponseIDIgnored: true,
				ErrorDropsAdvertisedID: true, ErrorReasonWrapsCause: true,
				SamplesProcessedInResponseOrder: true, DeduperCalledForEverySample: true,
				DeduperCompletesBeforeTriage: true, OnlyNovelHashesTriaged: true,
				NodeAddressRereadPerNovelHash: true, TriageBlocksInOrder: true,
				TriageCancellationAware: true, TriageCancellationBranchReturnsBeforePutFanout: true,
				ClampRequiresNovelAndOver300: true, ClampIntervalSeconds: 60,
				DurationConversion: "time.Duration(effective_signed_Go_int)*time.Second_with_int64_nanosecond_wrap",
				GoIntBits:          strconv.IntSize, IntervalCases: intervalCases,
				PutUsesAdvertisedIDAndCurrentAddr: true,
				PutOptionOrder:                    []string{"NodeResponded", "NodeBep51Support(true)", "NodeSampleInfoHashesRes"},
				PutDiscoveredCount:                "len(discoveredHashes)", PutTotalCount: "res.Num",
				PutDeadlineExpression:   "time.Now().Add(time.Duration(interval)*time.Second)",
				PutOccursAfterAllTriage: true, PutPrecedesFanoutLaunch: true,
				FanoutUsesResponseOrder: true, FanoutReadsCapturedResponseInGoroutine: true,
				FanoutDeepCopiesResponseNodes: false,
				FanoutDetached:                true, FanoutJoined: false, FanoutWholeListTimeoutMS: 1000,
				FanoutCancellationAware:         true,
				ProductionCapacity:              10 * int(config.ScalingFactor),
				ProductionConcurrency:           10 * int(config.ScalingFactor),
				DefaultScalingFactor:            int(config.ScalingFactor),
				ConsumerDequeuesBeforeSemaphore: true, ConsumerCallbacksDetached: true,
				AcquireCancellationDropsDequeuedItem: true,
				MaximumRetainedWork:                  "capacity_plus_concurrency_plus_one_acquire_waiter",
				ConsumerCallbacksJoined:              false, ClosedInputChecksOpenBoolean: false,
				ClosedInputOutcome:           "repeated_zero_value_callbacks_eventually_panic_on_nil_Node_accessor",
				ProductionTriageCapacity:     10 * int(config.ScalingFactor),
				ProductionTriageMaxBatchSize: 1000, ProductionTriageIntervalMS: 20_000,
				ProductionTriageOutputCapacity:  1,
				ProductionDiscoveryCapacity:     100 * int(config.ScalingFactor),
				ProductionDiscoveryMaxBatchSize: 10, ProductionDiscoveryIntervalMS: 10,
				ProductionDiscoveryOutputCapacity: 1,
				StartLaunchesWorkerDetached:       true, StartWaitsOnlyStopped: true,
				StartDefersSharedContextCancel: true, StartJoinsWorkerCallbacksOrFanout: false,
				FanoutCanOutliveWorkerPermit: true,
				PrerequisiteFixtureSHA256:    crawlerSampleInfoHashesWorkerPrerequisiteDigests(t),
				EvidenceCommit: map[string]string{
					"peer_sample_client_oracle":         "1f00b40705ba527721208023ddec64220fb40729",
					"ktable_temporal_oracle":            "1df4d7a09f74e13e75ea2e1ab1dcfc67a130ed9d",
					"sample_infohashes_producer_oracle": "602dce3287795bbe2eee89bbcc1e0ebc6f9c7701",
					"shared_sample_input_seam":          "e0fdd622f5869d092ff4322433d72bd17f783d11",
					"typed_info_hash_triage_route":      "b98da5ae34524f4b45c1bd0eee2e0d41dbd3128e",
					"ignore_hashes_oracle":              "684aedf68d9c07b96a362c470ec3619c0290b4f5",
					"rust_info_hash_deduper":            "accec9e0c0f89a3e5b64e8a60bb3f29393c13b52",
				},
				SourceSHA256: crawlerSampleInfoHashesWorkerSourceDigests(t),
				Nonclaims: []string{
					"exact_wall_clock_NodeResponded_or_next_sample_timestamp",
					"ready_select_tie_winner",
					"goroutine_callback_or_fanout_scheduling_order",
					"semaphore_or_mutex_fairness",
					"closed_buffered_input_runtime_execution",
					"callback_or_fanout_join_guarantee",
					"one_second_timeout_elapsed_in_runtime_rows",
					"exact_BoomFilters_random_decrement_offsets_or_retention",
					"exact_set_or_false_positive_false_negative_semantics",
					"production_batch_flush_timing_or_output_batch_boundaries",
					"infohash_triage_database_blocking_or_downstream_route_behavior",
					"discovered_node_deduplication_filtering_or_downstream_routing",
					"KTable_map_iteration_order_or_eviction_behavior",
					"opaque_NodeOption_function_identity_or_internal_field_layout",
					"live_DNS_UDP_or_DHT_network_behavior",
					"response_ID_as_advertised_node_identity",
					"Rust_implementation_public_API_or_overlapping_task_lifecycle",
					"Rust_signed_overflow_parity_for_interval_or_deadline_arithmetic",
				},
				Evidence: "runtime rows execute the actual worker with controlled interfaces; source-only facts bind full normalized AST and exact file hashes",
			},
		},
	}
}

func crawlerSampleInfoHashesWorkerInterval(name string, raw int64, novel int) crawlerSampleInfoHashesWorkerIntervalCase {
	effective := int(raw)
	if novel > 0 && effective > 300 {
		effective = 60
	}
	return crawlerSampleInfoHashesWorkerIntervalCase{
		Name: name, RawInterval: raw, NovelCount: novel,
		EffectiveInterval: int64(effective),
		DurationNS:        int64(time.Duration(effective) * time.Second),
	}
}

func runCrawlerSampleInfoHashesWorkerCandidateMutation(t *testing.T) crawlerSampleInfoHashesWorkerFixture {
	t.Helper()
	events := &crawlerSampleInfoHashesWorkerEventLog{}
	blockerGate := make(chan struct{})
	blockerEntered := make(chan struct{}, 1)
	blocker := crawlerSampleInfoHashesWorkerProbe(t, "permit_blocker", 41, "198.51.100.41:6941", false, events)
	blocker.candidateGate = blockerGate
	blocker.candidateEntered = blockerEntered
	targetReturned := make(chan struct{}, 1)
	target := crawlerSampleInfoHashesWorkerProbe(t, "mutated_target", 42, "198.51.100.42:6942", true, events)
	target.candidateReturned = targetReturned
	lane := concurrency.NewBufferedConcurrentChannel[ktable.Node](0, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := crawler{nodesForSampleInfoHashes: lane}
	done := make(chan struct{})
	go func() {
		c.runSampleInfoHashes(ctx)
		close(done)
	}()
	crawlerSampleInfoHashesWorkerSendNode(t, lane.In(), blocker, "permit blocker")
	crawlerSampleInfoHashesWorkerWait(t, blockerEntered, "permit blocker callback")
	crawlerSampleInfoHashesWorkerSendNode(t, lane.In(), target, "mutated target dequeue")
	events.append("target_dequeued_before_permit")
	target.setCandidate(false)
	close(blockerGate)
	crawlerSampleInfoHashesWorkerWait(t, targetReturned, "mutated target candidate return")
	cancel()
	crawlerSampleInfoHashesWorkerWait(t, done, "actual worker lane return")
	crawlerSampleInfoHashesWorkerWaitSourceGoroutinesExit(t)
	if calls := blocker.snapshotCalls(); calls != (crawlerSampleInfoHashesWorkerNodeCalls{SampleInfoHashesCandidate: 1}) {
		t.Fatalf("blocker calls = %+v", calls)
	}
	wantCalls := crawlerSampleInfoHashesWorkerNodeCalls{SampleInfoHashesCandidate: 1}
	if calls := target.snapshotCalls(); calls != wantCalls {
		t.Fatalf("mutated target calls = %+v, want %+v", calls, wantCalls)
	}
	return crawlerSampleInfoHashesWorkerFixture{
		ID: crawlerSampleInfoHashesWorkerFixtureIDs[1], Subsystem: "dht_crawler_sample_infohashes_worker",
		Classification: "RUNTIME_EXACT",
		Oracle: crawlerSampleInfoHashesWorkerOracle{
			Composition: "actual_runSampleInfoHashes_with_actual_capacity_zero_concurrency_one_buffered_lane",
			Determinism: "permit_blocker_proves_target_dequeued_then_mutated_before_callback",
			Lane:        "actual_production_BufferedConcurrentChannel_implementation_with_oracle_dimensions",
			Client:      "must_not_be_called", Deduper: "must_not_be_called", Table: "must_not_be_called",
			Triage: "must_not_be_called", Fanout: "must_not_be_called", Clock: "not_observed",
		},
		Input: crawlerSampleInfoHashesWorkerInput{
			Kind:         "actual_buffered_lane_callback_time_interface_node_candidate_mutation",
			LaneCapacity: 0, LaneConcurrency: 1, Node: crawlerSampleInfoHashesWorkerNodePtr(target.fixtureNode()),
			MutateCandidateAfterTake: true,
		},
		Expected: crawlerSampleInfoHashesWorkerExpected{
			NodeCalls: wantCalls, ClientCalls: []crawlerSampleInfoHashesWorkerClientCall{},
			SourceDerivedDeduperCallOrder: []string{}, DeduperPostMembership: map[string]bool{},
			TriageDeliveries: []crawlerSampleInfoHashesWorkerTriage{},
			Commands:         []crawlerSampleInfoHashesWorkerCommand{}, Discoveries: []crawlerSampleInfoHashesWorkerNode{},
			Events: events.snapshot(), RunReturned: true, ContextCancelled: true,
		},
	}
}

func runCrawlerSampleInfoHashesWorkerError(t *testing.T) crawlerSampleInfoHashesWorkerFixture {
	t.Helper()
	events := &crawlerSampleInfoHashesWorkerEventLog{}
	node := crawlerSampleInfoHashesWorkerProbe(t, "advertised_error_node", 51, "198.51.100.51:6951", true, events)
	response := crawlerSampleInfoHashesWorkerResponse{
		Kind: "error", ResponseID: crawlerPingWorkerID(251).String(),
		Samples: []string{crawlerPingWorkerID(151).String()},
		Nodes:   []crawlerSampleInfoHashesWorkerNode{crawlerSampleInfoHashesWorkerResponseNode(152, "203.0.113.152:7152")},
		Num:     700, Interval: 301,
	}
	target := crawlerPingWorkerID(211)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manual := &crawlerSampleInfoHashesWorkerManualLane{nodes: []ktable.Node{node}, events: events}
	scriptedClient := &crawlerSampleInfoHashesWorkerClient{
		wantContext: ctx, response: response, sameContext: true, events: events,
	}
	base := ktable.New(ktable.Params{NodeID: protocol.ID{}}).Table
	base.PutNode(node.id, node.addr)
	table := &crawlerSampleInfoHashesWorkerTracingTable{Table: base, events: events}
	c := crawler{
		nodesForSampleInfoHashes: manual, client: scriptedClient, kTable: table,
		ignoreHashes:    crawlerSampleInfoHashesWorkerFreshDeduper(),
		infoHashTriage:  crawlerSampleInfoHashesWorkerUnusedTriage(events),
		discoveredNodes: crawlerSampleInfoHashesWorkerUnusedDiscovery(events),
		soughtNodeID:    crawlerSampleInfoHashesWorkerTarget(target),
	}
	c.runSampleInfoHashes(ctx)
	if len(table.commands) != 1 || table.commands[0].Kind != "drop_node" ||
		table.commands[0].ID != node.id.String() ||
		table.commands[0].Reason != "sample_infohashes failed: oracle sample_infohashes failure" ||
		!table.commands[0].ErrorIdentityPreserved {
		t.Fatalf("error commands = %+v", table.commands)
	}
	wantCalls := crawlerSampleInfoHashesWorkerNodeCalls{ID: 1, Addr: 1, SampleInfoHashesCandidate: 1}
	if calls := node.snapshotCalls(); calls != wantCalls {
		t.Fatalf("error node calls = %+v, want %+v", calls, wantCalls)
	}
	return crawlerSampleInfoHashesWorkerFixture{
		ID: crawlerSampleInfoHashesWorkerFixtureIDs[2], Subsystem: "dht_crawler_sample_infohashes_worker",
		Classification: "RUNTIME_EXACT",
		Oracle: crawlerSampleInfoHashesWorkerOracle{
			Composition: "actual_runSampleInfoHashes_with_manual_callback_lane_scripted_error_and_actual_ktable",
			Determinism: "synchronous_callback_sentinel_error_and_no_downstream_sends",
			Lane:        "manual_single_callback", Client: "scripted_SampleInfoHashes_error",
			Deduper: "actual_fresh_production_filter_not_reached", Table: "tracing_wrapper_over_actual_ktable",
			Triage: "must_not_be_called", Fanout: "must_not_be_called", Clock: "not_observed",
		},
		Input: crawlerSampleInfoHashesWorkerInput{
			Kind: "eligible_client_error", Node: crawlerSampleInfoHashesWorkerNodePtr(node.fixtureNode()),
			Response: &response, SoughtTarget: target.String(),
		},
		Expected: crawlerSampleInfoHashesWorkerExpected{
			NodeCalls: wantCalls, ClientCalls: append([]crawlerSampleInfoHashesWorkerClientCall{}, scriptedClient.calls...),
			SameContext: scriptedClient.sameContext, SourceDerivedDeduperCallOrder: []string{},
			DeduperPostMembership: map[string]bool{},
			TriageDeliveries:      []crawlerSampleInfoHashesWorkerTriage{}, Commands: append([]crawlerSampleInfoHashesWorkerCommand{}, table.commands...),
			Discoveries: []crawlerSampleInfoHashesWorkerNode{}, Events: events.snapshot(),
			RunReturned: true, CallbackCompletionObserved: manual.completed,
		},
	}
}

func runCrawlerSampleInfoHashesWorkerTriageCancellation(t *testing.T) crawlerSampleInfoHashesWorkerFixture {
	t.Helper()
	const oracleRNGSeed = int64(1)
	// BoomFilters uses math/rand's package-global locked source. Reseeding immediately
	// before the bounded single-goroutine replay removes random-eviction flakes.
	rand.Seed(oracleRNGSeed)
	events := &crawlerSampleInfoHashesWorkerEventLog{}
	addresses := []string{
		"198.51.100.61:6961", "198.51.100.62:6962", "198.51.100.63:6963", "198.51.100.64:6964",
	}
	node := crawlerSampleInfoHashesWorkerProbe(t, "ordered_samples_node", 61, addresses[0], true, events)
	node.addrs = crawlerSampleInfoHashesWorkerAddresses(addresses)
	hashes := []string{
		"00000000000000000000000000000000000000a1",
		"00000000000000000000000000000000000000b2",
		"00000000000000000000000000000000000000c3",
		"00000000000000000000000000000000000000d4",
	}
	hashIndexes := crawlerSampleInfoHashesWorkerHashIndexes(hashes)
	crawlerSampleInfoHashesWorkerAssertDisjointIndexes(t, hashIndexes)
	response := crawlerSampleInfoHashesWorkerResponse{
		Kind: "success", ResponseID: crawlerPingWorkerID(252).String(), Samples: hashes,
		Nodes: []crawlerSampleInfoHashesWorkerNode{crawlerSampleInfoHashesWorkerResponseNode(162, "203.0.113.162:7162")},
		Num:   901, Interval: 301,
	}
	deduper := crawlerSampleInfoHashesWorkerFreshDeduper()
	preloaded := protocol.MustParseID(hashes[1])
	if deduper.testAndAdd(preloaded) {
		t.Fatal("fresh production deduper unexpectedly contained preload hash")
	}
	gate := make(chan struct{})
	entered := make(chan int, 3)
	triage := &crawlerSampleInfoHashesWorkerGatedBatching[nodeHasPeersForHash]{
		input: make(chan nodeHasPeersForHash, 2), entered: entered,
		gates: map[int]<-chan struct{}{3: gate}, events: events, label: "triage",
	}
	discovery := crawlerSampleInfoHashesWorkerUnusedDiscovery(events)
	target := crawlerPingWorkerID(212)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manual := &crawlerSampleInfoHashesWorkerManualLane{nodes: []ktable.Node{node}, events: events}
	scriptedClient := &crawlerSampleInfoHashesWorkerClient{wantContext: ctx, response: response, sameContext: true, events: events}
	table := &crawlerSampleInfoHashesWorkerTracingTable{
		Table: ktable.New(ktable.Params{NodeID: protocol.ID{}}).Table, events: events,
	}
	c := crawler{
		nodesForSampleInfoHashes: manual, client: scriptedClient, kTable: table,
		ignoreHashes: deduper, infoHashTriage: triage, discoveredNodes: discovery,
		soughtNodeID: crawlerSampleInfoHashesWorkerTarget(target),
	}
	done := make(chan struct{})
	go func() {
		c.runSampleInfoHashes(ctx)
		close(done)
	}()
	crawlerSampleInfoHashesWorkerWaitCall(t, entered, 3, "third triage In call")
	events.append("all_samples_deduped_before_cancel")
	cancel()
	events.append("context_cancelled")
	close(gate)
	crawlerSampleInfoHashesWorkerWait(t, done, "triage-cancelled worker return")
	postMembership := make(map[string]bool, len(hashes))
	for _, hash := range hashes {
		id := protocol.MustParseID(hash)
		postMembership[hash] = deduper.bloom.Test(id[:])
		if !postMembership[hash] {
			t.Fatalf("deduper did not retain processed hash %s under the pinned oracle RNG sequence", hash)
		}
	}
	deliveries := crawlerSampleInfoHashesWorkerDrainTriage(triage.input)
	wantDeliveries := []crawlerSampleInfoHashesWorkerTriage{
		{InfoHash: hashes[0], Node: addresses[1]},
		{InfoHash: hashes[2], Node: addresses[2]},
	}
	if !reflect.DeepEqual(deliveries, wantDeliveries) {
		t.Fatalf("triage prefix = %+v, want %+v", deliveries, wantDeliveries)
	}
	if len(table.commands) != 0 || discovery.callCount() != 0 {
		t.Fatalf("cancelled triage reached commands/fanout: commands=%+v fanout_calls=%d", table.commands, discovery.callCount())
	}
	wantCalls := crawlerSampleInfoHashesWorkerNodeCalls{Addr: 4, SampleInfoHashesCandidate: 1}
	if calls := node.snapshotCalls(); calls != wantCalls {
		t.Fatalf("triage node calls = %+v, want %+v", calls, wantCalls)
	}
	return crawlerSampleInfoHashesWorkerFixture{
		ID: crawlerSampleInfoHashesWorkerFixtureIDs[3], Subsystem: "dht_crawler_sample_infohashes_worker",
		Classification: "RUNTIME_EXACT",
		Oracle: crawlerSampleInfoHashesWorkerOracle{
			Composition: "actual_worker_actual_fresh_production_deduper_manual_lane_and_capacity_two_gated_triage",
			Determinism: "fixed_RNG_seed_disjoint_hash_vectors_full_buffer_and_cancel_only_ready_after_third_In_gate",
			Lane:        "manual_single_callback", Client: "scripted_ordered_success",
			Deduper: "actual_ignoreHashes_with_fresh_production_BoomFilters_and_preloaded_B",
			Table:   "tracing_actual_ktable_must_not_receive_command",
			Triage:  "oracle_capacity_two_input_with_gate_inside_third_In",
			Fanout:  "must_not_be_launched_after_triage_cancellation", Clock: "interval_not_reached",
		},
		Input: crawlerSampleInfoHashesWorkerInput{
			Kind: "ordered_samples_cancel_blocked_third_novel_triage", Node: crawlerSampleInfoHashesWorkerNodePtr(node.fixtureNode()),
			Response: &response, SoughtTarget: target.String(), PreloadedHashes: []string{hashes[1]},
			OracleRNGSeed: oracleRNGSeed, HashIndexes: hashIndexes,
			TriageCapacity: 2, CancelAtTriageInCall: 3,
		},
		Expected: crawlerSampleInfoHashesWorkerExpected{
			NodeCalls: wantCalls, ClientCalls: append([]crawlerSampleInfoHashesWorkerClientCall{}, scriptedClient.calls...),
			SameContext: scriptedClient.sameContext, SourceDerivedDeduperCallOrder: append([]string{}, hashes...),
			DeduperPostMembership: postMembership,
			TriageInCalls:         triage.callCount(), TriageDeliveries: deliveries,
			Commands: []crawlerSampleInfoHashesWorkerCommand{}, DiscoveryInCalls: 0,
			Discoveries: []crawlerSampleInfoHashesWorkerNode{}, Events: events.snapshot(),
			RunReturned: true, ContextCancelled: true, CallbackCompletionObserved: manual.completed,
		},
	}
}

func runCrawlerSampleInfoHashesWorkerFanoutCancellation(t *testing.T) crawlerSampleInfoHashesWorkerFixture {
	t.Helper()
	events := &crawlerSampleInfoHashesWorkerEventLog{}
	addresses := []string{"198.51.100.71:6971", "198.51.100.72:6972", "198.51.100.73:6973"}
	node := crawlerSampleInfoHashesWorkerProbe(t, "clamped_success_node", 71, addresses[0], true, events)
	node.addrs = crawlerSampleInfoHashesWorkerAddresses(addresses)
	hash := "00000000000000000000000000000000000000e5"
	responseNodes := []crawlerSampleInfoHashesWorkerNode{
		crawlerSampleInfoHashesWorkerResponseNode(171, "203.0.113.171:7171"),
		crawlerSampleInfoHashesWorkerResponseNode(172, "203.0.113.172:7172"),
		crawlerSampleInfoHashesWorkerResponseNode(173, "203.0.113.173:7173"),
		crawlerSampleInfoHashesWorkerResponseNode(174, "203.0.113.174:7174"),
	}
	response := crawlerSampleInfoHashesWorkerResponse{
		Kind: "success", ResponseID: crawlerPingWorkerID(253).String(), Samples: []string{hash},
		Nodes: responseNodes, Num: -17, Interval: 301,
	}
	triage := &crawlerSampleInfoHashesWorkerGatedBatching[nodeHasPeersForHash]{
		input: make(chan nodeHasPeersForHash, 1), gates: map[int]<-chan struct{}{}, events: events, label: "triage",
	}
	fanoutStart := make(chan struct{})
	thirdGate := make(chan struct{})
	discoveryEntered := make(chan int, 4)
	discovery := &crawlerSampleInfoHashesWorkerGatedBatching[ktable.Node]{
		input: make(chan ktable.Node, 2), entered: discoveryEntered,
		preGates: map[int]<-chan struct{}{1: fanoutStart},
		gates:    map[int]<-chan struct{}{3: thirdGate}, events: events, label: "discovery",
	}
	target := crawlerPingWorkerID(213)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manual := &crawlerSampleInfoHashesWorkerManualLane{nodes: []ktable.Node{node}, events: events}
	scriptedClient := &crawlerSampleInfoHashesWorkerClient{wantContext: ctx, response: response, sameContext: true, events: events}
	deduper := crawlerSampleInfoHashesWorkerFreshDeduper()
	base := ktable.New(ktable.Params{NodeID: protocol.ID{}}).Table
	base.PutNode(node.id, node.addr)
	table := &crawlerSampleInfoHashesWorkerTracingTable{Table: base, events: events}
	c := crawler{
		nodesForSampleInfoHashes: manual, client: scriptedClient, kTable: table,
		ignoreHashes: deduper, infoHashTriage: triage,
		discoveredNodes: discovery, soughtNodeID: crawlerSampleInfoHashesWorkerTarget(target),
	}
	c.runSampleInfoHashes(ctx)
	events.append("run_returned_before_fanout_send")
	close(fanoutStart)
	crawlerSampleInfoHashesWorkerWaitCall(t, discoveryEntered, 3, "third recursive discovery In call")
	cancel()
	events.append("context_cancelled")
	close(thirdGate)
	crawlerSampleInfoHashesWorkerWaitSourceGoroutinesExit(t)
	events.append("fanout_observed_exited")
	hashID := protocol.MustParseID(hash)
	if !deduper.bloom.Test(hashID[:]) {
		t.Fatal("clamped row novel hash missing from actual production deduper")
	}
	triageDeliveries := crawlerSampleInfoHashesWorkerDrainTriage(triage.input)
	wantTriage := []crawlerSampleInfoHashesWorkerTriage{{InfoHash: hash, Node: addresses[1]}}
	if !reflect.DeepEqual(triageDeliveries, wantTriage) {
		t.Fatalf("clamped triage = %+v, want %+v", triageDeliveries, wantTriage)
	}
	discoveries := crawlerSampleInfoHashesWorkerDrainDiscoveries(discovery.input)
	if !reflect.DeepEqual(discoveries, responseNodes[:2]) {
		t.Fatalf("recursive discovery prefix = %+v, want %+v", discoveries, responseNodes[:2])
	}
	if len(table.commands) != 1 || table.commands[0].Kind != "put_node" ||
		table.commands[0].ID != node.id.String() || table.commands[0].Addr != addresses[2] ||
		table.commands[0].OptionCount != 3 || !table.commands[0].StoredResponded || table.commands[0].StoredCandidate {
		t.Fatalf("clamped put command = %+v", table.commands)
	}
	wantCalls := crawlerSampleInfoHashesWorkerNodeCalls{ID: 1, Addr: 3, SampleInfoHashesCandidate: 1}
	if calls := node.snapshotCalls(); calls != wantCalls {
		t.Fatalf("clamped node calls = %+v, want %+v", calls, wantCalls)
	}
	return crawlerSampleInfoHashesWorkerFixture{
		ID: crawlerSampleInfoHashesWorkerFixtureIDs[4], Subsystem: "dht_crawler_sample_infohashes_worker",
		Classification: "RUNTIME_EXACT",
		Oracle: crawlerSampleInfoHashesWorkerOracle{
			Composition: "actual_worker_actual_deduper_tracing_actual_ktable_capacity_one_triage_and_gated_capacity_two_discovery",
			Determinism: "one_novel_forces_301_to_60_clamp_first_fanout_gate_proves_detachment_full_prefix_and_cancel_only_ready_at_third",
			Lane:        "manual_single_callback", Client: "scripted_success_with_one_novel_and_four_response_nodes",
			Deduper: "actual_fresh_production_ignoreHashes", Table: "tracing_wrapper_over_actual_ktable",
			Triage: "manual_capacity_one_input", Fanout: "oracle_gates_inside_first_and_third_discoveredNodes_In_calls",
			Clock: "runtime_asserts_responded_and_not_candidate_but_not_absolute_time",
		},
		Input: crawlerSampleInfoHashesWorkerInput{
			Kind: "clamp_put_then_detached_recursive_prefix_cancel", Node: crawlerSampleInfoHashesWorkerNodePtr(node.fixtureNode()),
			Response: &response, SoughtTarget: target.String(), TriageCapacity: 1,
			DiscoveryCapacity: 2, CancelAtDiscoveryInCall: 3,
		},
		Expected: crawlerSampleInfoHashesWorkerExpected{
			NodeCalls: wantCalls, ClientCalls: append([]crawlerSampleInfoHashesWorkerClientCall{}, scriptedClient.calls...),
			SameContext: scriptedClient.sameContext, SourceDerivedDeduperCallOrder: []string{hash},
			DeduperPostMembership: map[string]bool{hash: true},
			TriageInCalls:         triage.callCount(), TriageDeliveries: triageDeliveries,
			Commands:         append([]crawlerSampleInfoHashesWorkerCommand{}, table.commands...),
			DiscoveryInCalls: discovery.callCount(), Discoveries: discoveries, Events: events.snapshot(),
			RunReturned: true, ContextCancelled: true, CallbackCompletionObserved: manual.completed,
			FanoutCompletionObserved: true,
		},
	}
}

func crawlerSampleInfoHashesWorkerProbe(
	t *testing.T,
	token string,
	value int,
	address string,
	candidate bool,
	events *crawlerSampleInfoHashesWorkerEventLog,
) *crawlerSampleInfoHashesWorkerProbeNode {
	t.Helper()
	addr := netip.MustParseAddrPort(address)
	return &crawlerSampleInfoHashesWorkerProbeNode{
		token: token, id: crawlerPingWorkerID(value), addr: addr, addrs: []netip.AddrPort{addr},
		initialCandidate: candidate, candidate: candidate, events: events,
	}
}

func crawlerSampleInfoHashesWorkerResponseNode(value int, address string) crawlerSampleInfoHashesWorkerNode {
	return crawlerSampleInfoHashesWorkerNode{
		Token: fmt.Sprintf("response_%02x", byte(value)), ID: crawlerPingWorkerID(value).String(), Addr: address,
		InitialCandidate: true, FinalCandidate: true,
	}
}

func crawlerSampleInfoHashesWorkerNodePtr(node crawlerSampleInfoHashesWorkerNode) *crawlerSampleInfoHashesWorkerNode {
	return &node
}

func crawlerSampleInfoHashesWorkerAddresses(addresses []string) []netip.AddrPort {
	result := make([]netip.AddrPort, 0, len(addresses))
	for _, address := range addresses {
		result = append(result, netip.MustParseAddrPort(address))
	}
	return result
}

func crawlerSampleInfoHashesWorkerHashIndexes(hashes []string) map[string][]uint {
	result := make(map[string][]uint, len(hashes))
	for _, hashText := range hashes {
		id := protocol.MustParseID(hashText)
		hasher := fnv.New64()
		_, _ = hasher.Write(id[:])
		sum := hasher.Sum64()
		lower := uint32(sum & 0xffffffff)
		upper := uint32((sum >> 32) & 0xffffffff)
		indexes := make([]uint, crawlerIgnoreHashesDerivedK)
		for index := range indexes {
			indexes[index] = (uint(lower) + uint(upper)*uint(index)) % crawlerIgnoreHashesCells
		}
		result[hashText] = indexes
	}
	return result
}

func crawlerSampleInfoHashesWorkerAssertDisjointIndexes(t *testing.T, indexes map[string][]uint) {
	t.Helper()
	owners := make(map[uint]string)
	for hashText, hashIndexes := range indexes {
		if len(hashIndexes) != int(crawlerIgnoreHashesDerivedK) {
			t.Fatalf("hash %s index count = %d", hashText, len(hashIndexes))
		}
		for _, index := range hashIndexes {
			if owner, ok := owners[index]; ok {
				t.Fatalf("hashes %s and %s share Bloom index %d", owner, hashText, index)
			}
			owners[index] = hashText
		}
	}
}

func crawlerSampleInfoHashesWorkerTarget(target protocol.ID) *concurrency.AtomicValue[protocol.ID] {
	value := &concurrency.AtomicValue[protocol.ID]{}
	value.Set(target)
	return value
}

func crawlerSampleInfoHashesWorkerFreshDeduper() *ignoreHashes {
	return &ignoreHashes{bloom: boom.NewStableBloomFilter(
		crawlerIgnoreHashesCells,
		crawlerIgnoreHashesBitsPerCell,
		crawlerIgnoreHashesFalsePositiveRate,
	)}
}

func crawlerSampleInfoHashesWorkerUnusedTriage(
	events *crawlerSampleInfoHashesWorkerEventLog,
) *crawlerSampleInfoHashesWorkerGatedBatching[nodeHasPeersForHash] {
	return &crawlerSampleInfoHashesWorkerGatedBatching[nodeHasPeersForHash]{
		input: make(chan nodeHasPeersForHash), gates: map[int]<-chan struct{}{}, events: events, label: "triage",
	}
}

func crawlerSampleInfoHashesWorkerUnusedDiscovery(
	events *crawlerSampleInfoHashesWorkerEventLog,
) *crawlerSampleInfoHashesWorkerGatedBatching[ktable.Node] {
	return &crawlerSampleInfoHashesWorkerGatedBatching[ktable.Node]{
		input: make(chan ktable.Node), gates: map[int]<-chan struct{}{}, events: events, label: "discovery",
	}
}

func crawlerSampleInfoHashesWorkerSendNode(
	t *testing.T,
	input chan<- ktable.Node,
	node ktable.Node,
	label string,
) {
	t.Helper()
	select {
	case input <- node:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out sending %s", label)
	}
}

func crawlerSampleInfoHashesWorkerWait(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func crawlerSampleInfoHashesWorkerWaitCall(t *testing.T, entered <-chan int, want int, label string) {
	t.Helper()
	for {
		select {
		case call := <-entered:
			if call == want {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %s", label)
		}
	}
}

func crawlerSampleInfoHashesWorkerDrainTriage(
	input <-chan nodeHasPeersForHash,
) []crawlerSampleInfoHashesWorkerTriage {
	result := make([]crawlerSampleInfoHashesWorkerTriage, 0, len(input))
	for len(input) > 0 {
		item := <-input
		result = append(result, crawlerSampleInfoHashesWorkerTriage{
			InfoHash: item.infoHash.String(), Node: item.node.String(),
		})
	}
	return result
}

func crawlerSampleInfoHashesWorkerDrainDiscoveries(input <-chan ktable.Node) []crawlerSampleInfoHashesWorkerNode {
	result := make([]crawlerSampleInfoHashesWorkerNode, 0, len(input))
	for len(input) > 0 {
		node := <-input
		result = append(result, crawlerSampleInfoHashesWorkerNode{
			Token: "response_" + strings.TrimLeft(node.ID().String(), "0"),
			ID:    node.ID().String(), Addr: node.Addr().String(),
			InitialCandidate: node.IsSampleInfoHashesCandidate(), FinalCandidate: node.IsSampleInfoHashesCandidate(),
		})
	}
	return result
}

func crawlerSampleInfoHashesWorkerWaitSourceGoroutinesExit(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		bufferSize := 1 << 20
		var buffer []byte
		var length int
		for {
			buffer = make([]byte, bufferSize)
			length = runtime.Stack(buffer, true)
			if length < len(buffer) {
				break
			}
			if bufferSize >= 16<<20 {
				t.Fatal("runtime.Stack remained truncated while waiting for worker goroutine cleanup")
			}
			bufferSize *= 2
		}
		stacks := string(buffer[:length])
		if !strings.Contains(stacks, "sample_infohashes.go") {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("detached sample-infohashes recursive fanout goroutine did not exit")
}

func assertCrawlerSampleInfoHashesWorkerSourceShapes(t *testing.T) {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	sampleSet, sample := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/sample_infohashes.go"), "runSampleInfoHashes")
	wantSampleSet, wantSample := crawlerFindNodeWorkerParseSourceFunc(t, `package dhtcrawler
func (c *crawler) runSampleInfoHashes(ctx context.Context) {
	_ = c.nodesForSampleInfoHashes.Run(ctx, func(n ktable.Node) {
		if !n.IsSampleInfoHashesCandidate() { return }
		res, err := c.client.SampleInfoHashes(ctx, n.Addr(), c.soughtNodeID.Get())
		if err != nil {
			c.kTable.BatchCommand(ktable.DropNode{ID: n.ID(), Reason: fmt.Errorf("sample_infohashes failed: %w", err)},)
			return
		}
		var discoveredHashes []nodeHasPeersForHash
		for _, s := range res.Samples {
			if !c.ignoreHashes.testAndAdd(s) {
				discoveredHashes = append(discoveredHashes, nodeHasPeersForHash{infoHash: s, node: n.Addr()})
			}
		}
		for _, h := range discoveredHashes {
			select {
			case <-ctx.Done(): return
			case c.infoHashTriage.In() <- h: continue
			}
		}
		interval := res.Interval
		if len(discoveredHashes) > 0 && interval > 300 { interval = 60 }
		c.kTable.BatchCommand(ktable.PutNode{ID: n.ID(), Addr: n.Addr(), Options: []ktable.NodeOption{
			ktable.NodeResponded(),
			ktable.NodeBep51Support(true),
			ktable.NodeSampleInfoHashesRes(len(discoveredHashes), res.Num, time.Now().Add(time.Duration(interval)*time.Second),),
		}})
		if len(res.Nodes) > 0 {
			go func() {
				timeoutCtx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				for _, n := range res.Nodes {
					select {
					case <-timeoutCtx.Done(): return
					case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr): continue
					}
				}
			}()
		}
	})
}`)
	gotSampleText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, sampleSet, sample.Body))
	wantSampleText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, wantSampleSet, wantSample.Body))
	gotSampleText = strings.ReplaceAll(gotSampleText, ",:\x00):\x00", "):\x00")
	wantSampleText = strings.ReplaceAll(wantSampleText, ",:\x00):\x00", "):\x00")
	gotSampleText = strings.ReplaceAll(gotSampleText, ",:\x00}:\x00", "}:\x00")
	wantSampleText = strings.ReplaceAll(wantSampleText, ",:\x00}:\x00", "}:\x00")
	if gotSampleText != wantSampleText {
		t.Fatalf("runSampleInfoHashes normalized AST body changed\ngot: %q\nwant: %q", gotSampleText, wantSampleText)
	}

	factorySet, factory := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/factory.go"), "New")
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
	crawlerPingWorkerAssertExpr(t, factorySet, values["nodesForSampleInfoHashes"],
		`concurrency.NewBufferedConcurrentChannel[ktable.Node](
			10*scalingFactor,
			10*scalingFactor,
		)`)
	crawlerPingWorkerAssertExpr(t, factorySet, values["infoHashTriage"],
		"concurrency.NewBatchingChannel[nodeHasPeersForHash](10*scalingFactor, 1000, 20*time.Second)")
	crawlerPingWorkerAssertExpr(t, factorySet, values["ignoreHashes"],
		`&ignoreHashes{
			bloom: boom.NewStableBloomFilter(10_000_000, 2, 0.001),
		}`)

	startSet, start := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/crawler.go"), "start")
	startText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, startSet, start.Body))
	for _, required := range []string{
		"ctx,cancel:=context.WithCancel(context.Background())", "defer cancel()",
		"go c.runSampleInfoHashes(ctx)", "go c.runInfoHashTriage(ctx)", "<-c.stopped",
	} {
		if !strings.Contains(startText, crawlerPingWorkerTokenText(required)) {
			t.Fatalf("crawler start missing %s", required)
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
	crawlerFindNodeWorkerAssertBody(t, channelSet, channelRun, wantChannelSet, wantChannelRun,
		"BufferedConcurrentChannel.Run")

	batchSet, batchIn := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/concurrency/batching_channel.go"), "In")
	wantBatchSet, wantBatchIn := crawlerFindNodeWorkerParseSourceFunc(t, `package concurrency
func (ch *batchingChannel[T]) In() chan<- T { return ch.input }`)
	crawlerFindNodeWorkerAssertBody(t, batchSet, batchIn, wantBatchSet, wantBatchIn, "BatchingChannel.In")

	discoveredSet, discovered := crawlerPingWorkerParseFunc(t,
		filepath.Join(root, "internal/dhtcrawler/discovered_nodes.go"), "NewDiscoveredNodes")
	var discoveredConstructor ast.Expr
	ast.Inspect(discovered.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok && strings.Contains(crawlerPingWorkerASTText(t, discoveredSet, call.Fun), "NewBatchingChannel") {
			discoveredConstructor = call
			return false
		}
		return true
	})
	gotDiscovery := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, discoveredSet, discoveredConstructor))
	wantDiscovery := crawlerPingWorkerTokenText(
		"concurrency.NewBatchingChannel[ktable.Node](int(100*params.Config.ScalingFactor),10,time.Second/100)")
	if gotDiscovery != wantDiscovery {
		t.Fatalf("NewDiscoveredNodes constructor = %q, want %q", gotDiscovery, wantDiscovery)
	}

	nodePath := filepath.Join(root, "internal/protocol/dht/ktable/node.go")
	for _, name := range []string{"NodeResponded", "NodeBep51Support", "NodeSampleInfoHashesRes"} {
		if _, function := crawlerPingWorkerParseFunc(t, nodePath, name); function == nil {
			t.Fatalf("KTable option %s missing", name)
		}
	}
}

func crawlerSampleInfoHashesWorkerPrerequisiteDigests(t *testing.T) map[string]string {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	want := map[string]string{
		"testdata/parity/dht/peer_sample_client.jsonl":                     "8c432a1555587a0c3dff51af3191c689adb3a2eda8b6515975ee1470b4bdfe51",
		"testdata/parity/dht/dht_crawler_ignore_hashes.jsonl":              "7900b4046d10037b9c7541d36d79370a92ceb3135f9c81be0adef985ac1f4621",
		"testdata/parity/dht/ktable_temporal.jsonl":                        "03178e62efbc40519ccc0496204a081469ef49cf6b1a2336cff39b474a745444",
		"testdata/parity/dht/dht_crawler_sample_infohashes_producer.jsonl": "b0069a060b32edc4e1c6f5b2008f6b50f796eea6d162b4df3a148cad29745c1e",
	}
	for path, digest := range want {
		contents, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		actual := fmt.Sprintf("%x", sha256.Sum256(contents))
		if actual != digest {
			t.Fatalf("prerequisite %s SHA-256 = %s, want %s", path, actual, digest)
		}
	}
	return want
}

func crawlerSampleInfoHashesWorkerSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	paths := []string{
		"internal/concurrency/atomic.go",
		"internal/concurrency/batching_channel.go",
		"internal/concurrency/buffered_concurrent_channel.go",
		"internal/dhtcrawler/config.go",
		"internal/dhtcrawler/crawler.go",
		"internal/dhtcrawler/discovered_nodes.go",
		"internal/dhtcrawler/factory.go",
		"internal/dhtcrawler/sample_infohashes.go",
		"internal/protocol/dht/msg.go",
		"internal/protocol/dht/client/interface.go",
		"internal/protocol/dht/client/server_adapter.go",
		"internal/protocol/dht/ktable/command.go",
		"internal/protocol/dht/ktable/keyspace.go",
		"internal/protocol/dht/ktable/node.go",
		"internal/protocol/dht/ktable/query.go",
		"internal/protocol/dht/ktable/table.go",
		"internal/protocol/id.go",
	}
	digests := make(map[string]string, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		digests[path] = fmt.Sprintf("%x", sha256.Sum256(contents))
	}
	return digests
}

func reconcileCrawlerSampleInfoHashesWorkerFixtures(
	t *testing.T,
	fixtures []crawlerSampleInfoHashesWorkerFixture,
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
	digest := fmt.Sprintf("%x", sha256.Sum256(encoded.Bytes()))
	if crawlerSampleInfoHashesWorkerFixtureSHA256 != "" &&
		digest != crawlerSampleInfoHashesWorkerFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", digest, crawlerSampleInfoHashesWorkerFixtureSHA256)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve sample-infohashes worker generator source")
	}
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source), "../../testdata/parity/dht/dht_crawler_sample_infohashes_worker.jsonl"))
	if *updateDHTCrawlerSampleInfoHashesWorkerParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", digest)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-sample-infohashes-worker-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler sample-infohashes-worker fixture is stale; rerun with -update-dht-crawler-sample-infohashes-worker-parity")
	}
}

func (t *crawlerSampleInfoHashesWorkerTracingTable) BatchCommand(commands ...ktable.Command) {
	for _, command := range commands {
		switch command := command.(type) {
		case ktable.DropNode:
			t.commands = append(t.commands, crawlerSampleInfoHashesWorkerCommand{
				Kind: "drop_node", ID: command.ID.String(), Reason: command.Reason.Error(),
				ErrorIdentityPreserved: errors.Is(command.Reason, crawlerSampleInfoHashesWorkerSentinel),
			})
			t.events.append("table_drop_begin")
		case ktable.PutNode:
			t.commands = append(t.commands, crawlerSampleInfoHashesWorkerCommand{
				Kind: "put_node", ID: command.ID.String(), Addr: command.Addr.String(),
				OptionCount: len(command.Options),
			})
			t.events.append("table_put_begin")
		default:
			panic(fmt.Sprintf("unexpected sample-infohashes worker command %T", command))
		}
	}
	t.Table.BatchCommand(commands...)
	for index := range t.commands {
		command := &t.commands[index]
		if command.Kind != "put_node" || command.StoredResponded {
			continue
		}
		id := protocol.MustParseID(command.ID)
		for _, node := range t.Table.GetClosestNodes(id) {
			if node.ID() == id {
				command.StoredResponded = !node.Time().IsZero()
				command.StoredCandidate = node.IsSampleInfoHashesCandidate()
			}
		}
	}
	if len(commands) > 0 {
		kind := "put"
		if _, ok := commands[0].(ktable.DropNode); ok {
			kind = "drop"
		}
		t.events.append("table_" + kind + "_complete")
	}
}
