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
	"go/scanner"
	"go/token"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

var updateDHTCrawlerPingWorkerParity = flag.Bool(
	"update-dht-crawler-ping-worker-parity",
	false,
	"rewrite the Rust DHT crawler ping-worker parity fixture",
)

const crawlerPingWorkerFixtureSHA256 = "26d403becff0caeb0a27ec9027a366d51e19cdb7129ff05715cf24a6d2e1b040"

var crawlerPingWorkerFixtureIDs = [...]string{
	"production_factory_and_source_contract",
	"dropped_node_short_circuits_everything",
	"recent_node_skips_ping",
	"old_zero_id_success_learns_response_id",
	"old_matching_id_success_marks_responded",
	"old_mismatched_id_drops_advertised_id",
	"ping_error_drops_zero_not_advertised_id",
	"cancelled_after_success_still_puts",
	"lane_error_is_swallowed",
}

type crawlerPingWorkerFixture struct {
	ID        string                    `json:"id"`
	Subsystem string                    `json:"subsystem"`
	Oracle    crawlerPingWorkerOracle   `json:"oracle"`
	Input     crawlerPingWorkerInput    `json:"input"`
	Expected  crawlerPingWorkerExpected `json:"expected"`
}

type crawlerPingWorkerOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Lane        string `json:"lane"`
	Client      string `json:"client"`
	Table       string `json:"table"`
}

type crawlerPingWorkerInput struct {
	Kind               string                        `json:"kind"`
	Node               *crawlerPingWorkerNode        `json:"node,omitempty"`
	PingOutcome        string                        `json:"pingOutcome,omitempty"`
	ResponseID         string                        `json:"responseId,omitempty"`
	CancelBeforeReturn bool                          `json:"cancelBeforeReturn,omitempty"`
	LaneReturnError    bool                          `json:"laneReturnError,omitempty"`
	TableSetup         []crawlerPingWorkerTableSetup `json:"tableSetup,omitempty"`
}

type crawlerPingWorkerNode struct {
	ID    string                   `json:"id"`
	Addr  crawlerPingWorkerAddress `json:"addr"`
	State string                   `json:"state"`
}

type crawlerPingWorkerAddress struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope"`
}

type crawlerPingWorkerTableSetup struct {
	Kind string                   `json:"kind"`
	ID   string                   `json:"id"`
	Addr crawlerPingWorkerAddress `json:"addr"`
}

type crawlerPingWorkerExpected struct {
	NodeCalls              crawlerPingWorkerNodeCalls `json:"nodeCalls"`
	PingCalls              []crawlerPingWorkerAddress `json:"pingCalls"`
	SameContext            bool                       `json:"sameContext"`
	BatchCalls             int                        `json:"batchCalls"`
	Commands               []crawlerPingWorkerCommand `json:"commands"`
	RunReturned            bool                       `json:"runReturned"`
	ContextCancelled       bool                       `json:"contextCancelled"`
	AdvertisedNodeSurvived bool                       `json:"advertisedNodeSurvived"`
	Source                 *crawlerPingWorkerSource   `json:"source,omitempty"`
}

type crawlerPingWorkerNodeCalls struct {
	Dropped int `json:"dropped"`
	Time    int `json:"time"`
	ID      int `json:"id"`
	Addr    int `json:"addr"`
}

type crawlerPingWorkerCommand struct {
	Kind                   string                    `json:"kind"`
	ID                     string                    `json:"id"`
	Addr                   *crawlerPingWorkerAddress `json:"addr,omitempty"`
	OptionCount            int                       `json:"optionCount"`
	Reason                 string                    `json:"reason"`
	ErrorIdentityPreserved bool                      `json:"errorIdentityPreserved"`
	StoredResponded        bool                      `json:"storedResponded"`
}

type crawlerPingWorkerSource struct {
	RunErrorIgnored                  bool              `json:"runErrorIgnored"`
	GuardDroppedFirst                bool              `json:"guardDroppedFirst"`
	GuardUsesStrictAfter             bool              `json:"guardUsesStrictAfter"`
	ThresholdUsesNowMinusConfigured  bool              `json:"thresholdUsesNowMinusConfigured"`
	NodeIDInitializedZero            bool              `json:"nodeIdInitializedZero"`
	ErrorBeforeResponseProjection    bool              `json:"errorBeforeResponseProjection"`
	SuccessUsesNodeRespondedOption   bool              `json:"successUsesNodeRespondedOption"`
	NoPostPingCancellationCheck      bool              `json:"noPostPingCancellationCheck"`
	ProductionCapacity               int               `json:"productionCapacity"`
	ProductionConcurrency            int               `json:"productionConcurrency"`
	RunDequeuesBeforeAcquire         bool              `json:"runDequeuesBeforeAcquire"`
	RunSpawnsCallbacks               bool              `json:"runSpawnsCallbacks"`
	RunJoinsCallbacks                bool              `json:"runJoinsCallbacks"`
	GenericClosedInputRepeatsReceive bool              `json:"genericClosedInputRepeatsReceive"`
	PingClosedInputOutcome           string            `json:"pingClosedInputOutcome"`
	MaximumRetainedWork              string            `json:"maximumRetainedWork"`
	SourceSHA256                     map[string]string `json:"sourceSha256"`
	Evidence                         string            `json:"evidence"`
}

type crawlerPingWorkerScenario struct {
	id                 string
	node               *crawlerPingWorkerScriptedNode
	pingOutcome        string
	responseID         protocol.ID
	cancelBeforeReturn bool
	laneReturnError    bool
	seedAdvertisedNode bool
}

type crawlerPingWorkerScriptedNode struct {
	id        protocol.ID
	addr      netip.AddrPort
	dropped   bool
	responded time.Time
	panicTime bool
	calls     crawlerPingWorkerNodeCalls
}

func (n *crawlerPingWorkerScriptedNode) ID() protocol.ID {
	n.calls.ID++
	return n.id
}

func (n *crawlerPingWorkerScriptedNode) Addr() netip.AddrPort {
	n.calls.Addr++
	return n.addr
}

func (n *crawlerPingWorkerScriptedNode) Time() time.Time {
	n.calls.Time++
	if n.panicTime {
		panic("Time must not be evaluated after Dropped returns true")
	}
	return n.responded
}

func (n *crawlerPingWorkerScriptedNode) Dropped() bool {
	n.calls.Dropped++
	return n.dropped
}

func (*crawlerPingWorkerScriptedNode) IsSampleInfoHashesCandidate() bool {
	panic("ping worker must not inspect sample_infohashes eligibility")
}

type crawlerPingWorkerManualLane struct {
	node      ktable.Node
	returnErr error
}

func (*crawlerPingWorkerManualLane) In() chan<- ktable.Node {
	panic("ping worker must not request the lane sender")
}

func (l *crawlerPingWorkerManualLane) Run(ctx context.Context, callback func(ktable.Node)) error {
	if l.node != nil {
		callback(l.node)
	}
	return l.returnErr
}

type crawlerPingWorkerClient struct {
	client.Client
	wantContext        context.Context
	response           client.PingResult
	err                error
	cancelBeforeReturn context.CancelFunc
	calls              []crawlerPingWorkerAddress
	sameContext        bool
}

func (c *crawlerPingWorkerClient) Ping(
	ctx context.Context,
	addr netip.AddrPort,
) (client.PingResult, error) {
	c.calls = append(c.calls, projectCrawlerPingWorkerAddress(addr))
	c.sameContext = ctx == c.wantContext
	if c.cancelBeforeReturn != nil {
		c.cancelBeforeReturn()
	}
	return c.response, c.err
}

type crawlerPingWorkerTracingTable struct {
	ktable.Table
	sentinel   error
	batchCalls int
	commands   []crawlerPingWorkerCommand
}

func (t *crawlerPingWorkerTracingTable) BatchCommand(commands ...ktable.Command) {
	t.batchCalls++
	start := len(t.commands)
	for _, command := range commands {
		switch command := command.(type) {
		case ktable.PutNode:
			addr := projectCrawlerPingWorkerAddress(command.Addr)
			t.commands = append(t.commands, crawlerPingWorkerCommand{
				Kind: "put_node", ID: command.ID.String(), Addr: &addr,
				OptionCount: len(command.Options),
			})
		case ktable.DropNode:
			t.commands = append(t.commands, crawlerPingWorkerCommand{
				Kind: "drop_node", ID: command.ID.String(), Reason: command.Reason.Error(),
				ErrorIdentityPreserved: errors.Is(command.Reason, t.sentinel),
			})
		default:
			panic(fmt.Sprintf("unexpected ping worker command %T", command))
		}
	}
	t.Table.BatchCommand(commands...)
	for index := start; index < len(t.commands); index++ {
		command := &t.commands[index]
		if command.Kind != "put_node" {
			continue
		}
		id := protocol.MustParseID(command.ID)
		for _, node := range t.Table.GetClosestNodes(id) {
			if node.ID() == id {
				command.StoredResponded = !node.Time().IsZero()
				break
			}
		}
	}
}

func TestGenerateDHTCrawlerPingWorkerParity(t *testing.T) {
	sentinel := errors.New("oracle ping failure")
	fixtures := []crawlerPingWorkerFixture{crawlerPingWorkerSourceFixture(t)}
	fixtures = append(fixtures,
		runCrawlerPingWorkerScenario(t, sentinel, crawlerPingWorkerScenario{
			id:   "dropped_node_short_circuits_everything",
			node: newCrawlerPingWorkerNode(1, "192.0.2.1", 6881, "dropped"),
		}),
		runCrawlerPingWorkerScenario(t, sentinel, crawlerPingWorkerScenario{
			id:   "recent_node_skips_ping",
			node: newCrawlerPingWorkerNode(2, "192.0.2.2", 6882, "recent"),
		}),
		runCrawlerPingWorkerScenario(t, sentinel, crawlerPingWorkerScenario{
			id:          "old_zero_id_success_learns_response_id",
			node:        newCrawlerPingWorkerNode(0, "198.51.100.3", 6883, "old"),
			pingOutcome: "success", responseID: crawlerPingWorkerID(3),
		}),
		runCrawlerPingWorkerScenario(t, sentinel, crawlerPingWorkerScenario{
			id:          "old_matching_id_success_marks_responded",
			node:        newCrawlerPingWorkerNode(4, "198.51.100.4", 6884, "old"),
			pingOutcome: "success", responseID: crawlerPingWorkerID(4),
		}),
		runCrawlerPingWorkerScenario(t, sentinel, crawlerPingWorkerScenario{
			id:          "old_mismatched_id_drops_advertised_id",
			node:        newCrawlerPingWorkerNode(5, "203.0.113.5", 6885, "old"),
			pingOutcome: "success", responseID: crawlerPingWorkerID(55),
			seedAdvertisedNode: true,
		}),
		runCrawlerPingWorkerScenario(t, sentinel, crawlerPingWorkerScenario{
			id:          "ping_error_drops_zero_not_advertised_id",
			node:        newCrawlerPingWorkerNode(6, "203.0.113.6", 6886, "old"),
			pingOutcome: "error", responseID: crawlerPingWorkerID(66),
			seedAdvertisedNode: true,
		}),
		runCrawlerPingWorkerScenario(t, sentinel, crawlerPingWorkerScenario{
			id:          "cancelled_after_success_still_puts",
			node:        newCrawlerPingWorkerNode(7, "203.0.113.7", 6887, "old"),
			pingOutcome: "success", responseID: crawlerPingWorkerID(7),
			cancelBeforeReturn: true,
		}),
		runCrawlerPingWorkerScenario(t, sentinel, crawlerPingWorkerScenario{
			id:              "lane_error_is_swallowed",
			laneReturnError: true,
		}),
	)

	if len(fixtures) != len(crawlerPingWorkerFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerPingWorkerFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerPingWorkerFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerPingWorkerFixtureIDs[index])
		}
	}
	reconcileCrawlerPingWorkerFixtures(t, fixtures)
}

func crawlerPingWorkerSourceFixture(t *testing.T) crawlerPingWorkerFixture {
	t.Helper()
	assertCrawlerPingWorkerSourceShapes(t)
	defaultScalingFactor := int(NewDefaultConfig().ScalingFactor)
	if defaultScalingFactor != 10 {
		t.Fatalf("default crawler scaling factor = %d, want 10", defaultScalingFactor)
	}
	return crawlerPingWorkerFixture{
		ID: "production_factory_and_source_contract", Subsystem: "dht_crawler_ping",
		Oracle: crawlerPingWorkerOracle{
			Composition: "source_and_factory_freshness_gate",
			Determinism: "exact_source_sha256_and_required_source_shapes",
			Lane:        "production_buffered_concurrent_channel",
			Client:      "production_dht_client_interface",
			Table:       "production_ktable_batch_command",
		},
		Input: crawlerPingWorkerInput{Kind: "source_contract"},
		Expected: crawlerPingWorkerExpected{
			PingCalls: []crawlerPingWorkerAddress{}, Commands: []crawlerPingWorkerCommand{},
			RunReturned: true,
			Source: &crawlerPingWorkerSource{
				RunErrorIgnored:                  true,
				GuardDroppedFirst:                true,
				GuardUsesStrictAfter:             true,
				ThresholdUsesNowMinusConfigured:  true,
				NodeIDInitializedZero:            true,
				ErrorBeforeResponseProjection:    true,
				SuccessUsesNodeRespondedOption:   true,
				NoPostPingCancellationCheck:      true,
				ProductionCapacity:               defaultScalingFactor,
				ProductionConcurrency:            defaultScalingFactor,
				RunDequeuesBeforeAcquire:         true,
				RunSpawnsCallbacks:               true,
				RunJoinsCallbacks:                false,
				GenericClosedInputRepeatsReceive: true,
				PingClosedInputOutcome:           "nil_node_callback_panics_process",
				MaximumRetainedWork:              "capacity_plus_concurrency_plus_one_acquire_waiter",
				SourceSHA256:                     crawlerPingWorkerSourceDigests(t),
				Evidence:                         "real runPing rows plus exact Go source freshness; production executor facts are source-shaped because callback scheduling is nondeterministic",
			},
		},
	}
}

func runCrawlerPingWorkerScenario(
	t *testing.T,
	sentinel error,
	scenario crawlerPingWorkerScenario,
) crawlerPingWorkerFixture {
	t.Helper()
	origin := crawlerPingWorkerID(250)
	table := ktable.New(ktable.Params{NodeID: origin}).Table
	if scenario.seedAdvertisedNode {
		table.PutNode(scenario.node.id, scenario.node.addr)
	}
	tracingTable := &crawlerPingWorkerTracingTable{Table: table, sentinel: sentinel}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pingClient := &crawlerPingWorkerClient{
		wantContext: ctx,
		response:    client.PingResult{ID: scenario.responseID},
	}
	if scenario.pingOutcome == "error" {
		pingClient.err = sentinel
	}
	if scenario.cancelBeforeReturn {
		pingClient.cancelBeforeReturn = cancel
	}
	lane := &crawlerPingWorkerManualLane{}
	if scenario.node != nil {
		lane.node = scenario.node
	}
	if scenario.laneReturnError {
		lane.returnErr = errors.New("oracle lane failure")
	}
	c := crawler{
		kTable:           tracingTable,
		client:           pingClient,
		nodesForPing:     lane,
		oldPeerThreshold: 15 * time.Minute,
	}
	c.runPing(ctx)

	input := crawlerPingWorkerInput{
		Kind: "run_ping", PingOutcome: scenario.pingOutcome,
		ResponseID:         scenario.responseID.String(),
		CancelBeforeReturn: scenario.cancelBeforeReturn,
		LaneReturnError:    scenario.laneReturnError,
	}
	nodeCalls := crawlerPingWorkerNodeCalls{}
	advertisedSurvived := false
	if scenario.node != nil {
		node := projectCrawlerPingWorkerNode(scenario.node)
		input.Node = &node
		nodeCalls = scenario.node.calls
		advertisedSurvived = crawlerPingWorkerTableContains(table, scenario.node.id)
		if scenario.seedAdvertisedNode {
			input.TableSetup = []crawlerPingWorkerTableSetup{{
				Kind: "put_node", ID: scenario.node.id.String(),
				Addr: projectCrawlerPingWorkerAddress(scenario.node.addr),
			}}
		}
	}
	return crawlerPingWorkerFixture{
		ID: scenario.id, Subsystem: "dht_crawler_ping",
		Oracle: crawlerPingWorkerOracle{
			Composition: "actual_crawler_runPing_with_manual_single_callback_lane",
			Determinism: "synchronous_callback_and_scripted_client",
			Lane:        "manual_single_callback_interface_implementation",
			Client:      "scripted_client_Client_ping_override",
			Table:       "tracing_wrapper_over_actual_ktable",
		},
		Input: input,
		Expected: crawlerPingWorkerExpected{
			NodeCalls:              nodeCalls,
			PingCalls:              append([]crawlerPingWorkerAddress{}, pingClient.calls...),
			SameContext:            pingClient.sameContext,
			BatchCalls:             tracingTable.batchCalls,
			Commands:               append([]crawlerPingWorkerCommand{}, tracingTable.commands...),
			RunReturned:            true,
			ContextCancelled:       ctx.Err() != nil,
			AdvertisedNodeSurvived: advertisedSurvived,
		},
	}
}

func newCrawlerPingWorkerNode(
	value int,
	ip string,
	port uint16,
	state string,
) *crawlerPingWorkerScriptedNode {
	node := &crawlerPingWorkerScriptedNode{
		id:   crawlerPingWorkerID(value),
		addr: netip.MustParseAddrPort(fmt.Sprintf("%s:%d", ip, port)),
	}
	switch state {
	case "dropped":
		node.dropped = true
		node.panicTime = true
	case "recent":
		node.responded = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	case "old":
		node.responded = time.Time{}
	default:
		panic("unknown scripted ping node state")
	}
	return node
}

func projectCrawlerPingWorkerNode(node *crawlerPingWorkerScriptedNode) crawlerPingWorkerNode {
	state := "old"
	if node.dropped {
		state = "dropped"
	} else if !node.responded.IsZero() {
		state = "recent"
	}
	return crawlerPingWorkerNode{
		ID: node.id.String(), Addr: projectCrawlerPingWorkerAddress(node.addr), State: state,
	}
}

func projectCrawlerPingWorkerAddress(addr netip.AddrPort) crawlerPingWorkerAddress {
	scope, _ := strconv.ParseUint(addr.Addr().Zone(), 10, 32)
	return crawlerPingWorkerAddress{
		IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: uint32(scope),
	}
}

func crawlerPingWorkerID(value int) protocol.ID {
	var id protocol.ID
	id[18] = byte(value >> 8)
	id[19] = byte(value)
	return id
}

func crawlerPingWorkerTableContains(table ktable.Table, id protocol.ID) bool {
	for _, node := range table.GetClosestNodes(id) {
		if node.ID() == id {
			return true
		}
	}
	return false
}

func assertCrawlerPingWorkerSourceShapes(t *testing.T) {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	pingSet, ping := crawlerPingWorkerParseFunc(
		t, filepath.Join(root, "internal/dhtcrawler/ping.go"), "runPing",
	)
	if len(ping.Body.List) != 1 {
		t.Fatalf("runPing body has %d statements, want one Run assignment", len(ping.Body.List))
	}
	runAssign, ok := ping.Body.List[0].(*ast.AssignStmt)
	if !ok || runAssign.Tok != token.ASSIGN || len(runAssign.Lhs) != 1 ||
		crawlerPingWorkerASTText(t, pingSet, runAssign.Lhs[0]) != "_" || len(runAssign.Rhs) != 1 {
		t.Fatal("runPing no longer explicitly ignores exactly one Run result")
	}
	runCall, ok := runAssign.Rhs[0].(*ast.CallExpr)
	if !ok || crawlerPingWorkerASTText(t, pingSet, runCall.Fun) != "c.nodesForPing.Run" ||
		len(runCall.Args) != 2 || crawlerPingWorkerASTText(t, pingSet, runCall.Args[0]) != "ctx" {
		t.Fatal("runPing no longer calls nodesForPing.Run with the shared context")
	}
	callback, ok := runCall.Args[1].(*ast.FuncLit)
	if !ok || len(callback.Body.List) != 5 {
		t.Fatal("runPing callback no longer has the frozen five-statement shape")
	}
	guard, ok := callback.Body.List[0].(*ast.IfStmt)
	if !ok || guard.Else != nil || len(guard.Body.List) != 1 {
		t.Fatal("runPing dropped/recent guard shape changed")
	}
	crawlerPingWorkerAssertExpr(t, pingSet, guard.Cond,
		"n.Dropped() || n.Time().After(time.Now().Add(-c.oldPeerThreshold))")
	guardReturn, ok := guard.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(guardReturn.Results) != 0 {
		t.Fatal("runPing guard no longer returns without a result")
	}
	pingAssign, ok := callback.Body.List[1].(*ast.AssignStmt)
	if !ok || pingAssign.Tok != token.DEFINE || len(pingAssign.Lhs) != 2 ||
		crawlerPingWorkerASTText(t, pingSet, pingAssign.Lhs[0]) != "res" ||
		crawlerPingWorkerASTText(t, pingSet, pingAssign.Lhs[1]) != "err" || len(pingAssign.Rhs) != 1 {
		t.Fatal("runPing query assignment shape changed")
	}
	crawlerPingWorkerAssertExpr(t, pingSet, pingAssign.Rhs[0], "c.client.Ping(ctx, n.Addr())")
	declaration, ok := callback.Body.List[2].(*ast.DeclStmt)
	if !ok || crawlerPingWorkerASTText(t, pingSet, declaration) != "var nodeID protocol.ID" {
		t.Fatal("runPing nodeID is no longer zero-initialized before result projection")
	}
	success, ok := callback.Body.List[3].(*ast.IfStmt)
	if !ok || success.Else != nil || len(success.Body.List) != 2 {
		t.Fatal("runPing success projection shape changed")
	}
	crawlerPingWorkerAssertExpr(t, pingSet, success.Cond, "err == nil")
	crawlerPingWorkerAssertAssign(t, pingSet, success.Body.List[0], "nodeID", "res.ID")
	mismatch, ok := success.Body.List[1].(*ast.IfStmt)
	if !ok || mismatch.Else != nil || len(mismatch.Body.List) != 2 {
		t.Fatal("runPing mismatch branch shape changed")
	}
	crawlerPingWorkerAssertExpr(t, pingSet, mismatch.Cond,
		"!n.ID().IsZero() && n.ID() != nodeID")
	crawlerPingWorkerAssertAssign(t, pingSet, mismatch.Body.List[0], "nodeID", "n.ID()")
	crawlerPingWorkerAssertAssign(t, pingSet, mismatch.Body.List[1], "err",
		`errors.New("node responded with a mismatching ID")`)
	commandBranch, ok := callback.Body.List[4].(*ast.IfStmt)
	if !ok || commandBranch.Else == nil || len(commandBranch.Body.List) != 1 {
		t.Fatal("runPing table-command branch shape changed")
	}
	elseBlock, ok := commandBranch.Else.(*ast.BlockStmt)
	if !ok || len(elseBlock.List) != 1 {
		t.Fatal("runPing success table-command branch shape changed")
	}
	crawlerPingWorkerAssertExpr(t, pingSet, commandBranch.Cond, "err != nil")
	dropCall := crawlerPingWorkerOnlyExprCall(t, commandBranch.Body.List[0])
	if crawlerPingWorkerASTText(t, pingSet, dropCall.Fun) != "c.kTable.BatchCommand" ||
		len(dropCall.Args) != 1 {
		t.Fatal("runPing error branch no longer issues exactly one BatchCommand")
	}
	crawlerPingWorkerAssertExpr(t, pingSet, dropCall.Args[0], `ktable.DropNode{
		ID:     nodeID,
		Reason: fmt.Errorf("failed to respond to ping: %w", err),
	}`)
	putCall := crawlerPingWorkerOnlyExprCall(t, elseBlock.List[0])
	if crawlerPingWorkerASTText(t, pingSet, putCall.Fun) != "c.kTable.BatchCommand" ||
		len(putCall.Args) != 1 {
		t.Fatal("runPing success branch no longer issues exactly one BatchCommand")
	}
	crawlerPingWorkerAssertExpr(t, pingSet, putCall.Args[0], `ktable.PutNode{
		ID:      nodeID,
		Addr:    n.Addr(),
		Options: []ktable.NodeOption{ktable.NodeResponded()},
	}`)

	factorySet, factory := crawlerPingWorkerParseFunc(
		t, filepath.Join(root, "internal/dhtcrawler/factory.go"), "New",
	)
	factoryValues := make(map[string]ast.Expr)
	ast.Inspect(factory.Body, func(node ast.Node) bool {
		entry, ok := node.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		key, ok := entry.Key.(*ast.Ident)
		if ok && (key.Name == "nodesForPing" || key.Name == "oldPeerThreshold") {
			factoryValues[key.Name] = entry.Value
		}
		return true
	})
	crawlerPingWorkerAssertExpr(t, factorySet, factoryValues["nodesForPing"],
		"concurrency.NewBufferedConcurrentChannel[ktable.Node](scalingFactor, scalingFactor)")
	crawlerPingWorkerAssertExpr(t, factorySet, factoryValues["oldPeerThreshold"], "time.Minute * 15")

	channelSet, channelRun := crawlerPingWorkerParseFunc(
		t,
		filepath.Join(root, "internal/concurrency/buffered_concurrent_channel.go"),
		"Run",
	)
	if len(channelRun.Body.List) != 1 {
		t.Fatal("BufferedConcurrentChannel.Run no longer consists of one owning loop")
	}
	loop, ok := channelRun.Body.List[0].(*ast.ForStmt)
	if !ok || loop.Cond != nil || len(loop.Body.List) != 1 {
		t.Fatal("BufferedConcurrentChannel.Run loop shape changed")
	}
	selection, ok := loop.Body.List[0].(*ast.SelectStmt)
	if !ok || len(selection.Body.List) != 2 {
		t.Fatal("BufferedConcurrentChannel.Run select shape changed")
	}
	var receiveClause *ast.CommClause
	for _, statement := range selection.Body.List {
		clause := statement.(*ast.CommClause)
		assignment, ok := clause.Comm.(*ast.AssignStmt)
		if !ok || len(assignment.Rhs) != 1 {
			continue
		}
		if crawlerPingWorkerASTText(t, channelSet, assignment.Rhs[0]) == "<-ch.ch" {
			if assignment.Tok != token.DEFINE || len(assignment.Lhs) != 1 ||
				crawlerPingWorkerASTText(t, channelSet, assignment.Lhs[0]) != "next" {
				t.Fatal("closed-channel receive gained an open boolean or changed target")
			}
			receiveClause = clause
		}
	}
	if receiveClause == nil || len(receiveClause.Body) != 2 {
		t.Fatal("BufferedConcurrentChannel receive clause shape changed")
	}
	acquire, ok := receiveClause.Body[0].(*ast.IfStmt)
	if !ok || acquire.Init == nil || len(acquire.Body.List) != 1 {
		t.Fatal("BufferedConcurrentChannel no longer acquires after dequeue")
	}
	if crawlerPingWorkerASTText(t, channelSet, acquire.Init) !=
		"err := ch.sem.Acquire(ctx, 1)" {
		t.Fatal("BufferedConcurrentChannel semaphore acquisition changed")
	}
	crawlerPingWorkerAssertExpr(t, channelSet, acquire.Cond, "err != nil")
	goStatement, ok := receiveClause.Body[1].(*ast.GoStmt)
	if !ok {
		t.Fatal("BufferedConcurrentChannel callback is no longer detached with go")
	}
	callbackCall, ok := goStatement.Call.Fun.(*ast.FuncLit)
	if !ok || len(callbackCall.Body.List) != 2 || len(goStatement.Call.Args) != 0 {
		t.Fatal("BufferedConcurrentChannel detached callback body changed")
	}
	if crawlerPingWorkerASTText(t, channelSet, callbackCall.Body.List[0]) !=
		"defer ch.sem.Release(1)" ||
		crawlerPingWorkerASTText(t, channelSet, callbackCall.Body.List[1]) != "f(next)" {
		t.Fatal("BufferedConcurrentChannel callback release/invocation order changed")
	}
}

func crawlerPingWorkerParseFunc(
	t *testing.T,
	path string,
	name string,
) (*token.FileSet, *ast.FuncDecl) {
	t.Helper()
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name {
			return fileSet, function
		}
	}
	t.Fatalf("function %s not found in %s", name, path)
	return nil, nil
}

func crawlerPingWorkerASTText(t *testing.T, fileSet *token.FileSet, node any) string {
	t.Helper()
	var output bytes.Buffer
	if err := format.Node(&output, fileSet, node); err != nil {
		t.Fatal(err)
	}
	return output.String()
}

func crawlerPingWorkerAssertExpr(
	t *testing.T,
	actualSet *token.FileSet,
	actual ast.Expr,
	expected string,
) {
	t.Helper()
	if actual == nil {
		t.Fatalf("missing expression, want %s", expected)
	}
	expectedSet := token.NewFileSet()
	expectedExpr, err := parser.ParseExprFrom(expectedSet, "expected.go", expected, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := crawlerPingWorkerASTText(t, actualSet, actual)
	want := crawlerPingWorkerASTText(t, expectedSet, expectedExpr)
	if crawlerPingWorkerTokenText(got) != crawlerPingWorkerTokenText(want) {
		t.Fatalf("expression = %s, want %s", got, want)
	}
}

func crawlerPingWorkerTokenText(source string) string {
	fileSet := token.NewFileSet()
	file := fileSet.AddFile("expression.go", -1, len(source))
	var lexer scanner.Scanner
	lexer.Init(file, []byte(source), nil, 0)
	var tokens strings.Builder
	for {
		_, current, literal := lexer.Scan()
		if current == token.EOF {
			return tokens.String()
		}
		tokens.WriteString(current.String())
		tokens.WriteByte(':')
		tokens.WriteString(literal)
		tokens.WriteByte(0)
	}
}

func crawlerPingWorkerAssertAssign(
	t *testing.T,
	fileSet *token.FileSet,
	statement ast.Stmt,
	left string,
	right string,
) {
	t.Helper()
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok || assignment.Tok != token.ASSIGN || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 ||
		crawlerPingWorkerASTText(t, fileSet, assignment.Lhs[0]) != left {
		t.Fatalf("assignment no longer has exact target %s", left)
	}
	crawlerPingWorkerAssertExpr(t, fileSet, assignment.Rhs[0], right)
}

func crawlerPingWorkerOnlyExprCall(t *testing.T, statement ast.Stmt) *ast.CallExpr {
	t.Helper()
	expression, ok := statement.(*ast.ExprStmt)
	if !ok {
		t.Fatal("expected one call expression statement")
	}
	call, ok := expression.X.(*ast.CallExpr)
	if !ok {
		t.Fatal("expected call expression")
	}
	return call
}

func crawlerPingWorkerSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	paths := []string{
		"internal/concurrency/buffered_concurrent_channel.go",
		"internal/dhtcrawler/config.go",
		"internal/dhtcrawler/crawler.go",
		"internal/dhtcrawler/factory.go",
		"internal/dhtcrawler/ping.go",
		"internal/protocol/dht/client/interface.go",
		"internal/protocol/dht/ktable/command.go",
		"internal/protocol/dht/ktable/node.go",
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

func crawlerPingWorkerRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve ping-worker generator source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
}

func reconcileCrawlerPingWorkerFixtures(t *testing.T, fixtures []crawlerPingWorkerFixture) {
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
	if crawlerPingWorkerFixtureSHA256 != "" && actualHash != crawlerPingWorkerFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerPingWorkerFixtureSHA256)
	}
	path := filepath.Join(crawlerPingWorkerRoot(t), "testdata/parity/dht/dht_crawler_ping_worker.jsonl")
	if *updateDHTCrawlerPingWorkerParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-ping-worker-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler ping-worker fixture is stale; rerun with -update-dht-crawler-ping-worker-parity")
	}
}

var (
	_ concurrency.BufferedConcurrentChannel[ktable.Node] = (*crawlerPingWorkerManualLane)(nil)
	_ client.Client                                      = (*crawlerPingWorkerClient)(nil)
	_ ktable.Table                                       = (*crawlerPingWorkerTracingTable)(nil)
)
