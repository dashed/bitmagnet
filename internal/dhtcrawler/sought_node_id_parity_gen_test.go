package dhtcrawler

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

var updateDHTCrawlerSoughtNodeIDParity = flag.Bool(
	"update-dht-crawler-sought-node-id-parity",
	false,
	"rewrite the Rust DHT crawler shared sought-node-ID parity fixture",
)

const crawlerSoughtNodeIDFixtureSHA256 = "b930aeb8e248ad419174296cb3348efa48f5f89b02c70f426e95a839f2d0ce91"

var crawlerSoughtNodeIDFixtureIDs = [...]string{
	"production_shared_target_source_contract",
	"zero_value_get_returns_zero_id",
	"set_then_get_returns_exact_id",
	"aliases_observe_replacement_a_to_b",
	"controlled_cross_goroutine_whole_value_handoff",
}

const (
	crawlerSoughtNodeIDZero = "0000000000000000000000000000000000000000"
	crawlerSoughtNodeIDA    = "00112233445566778899aabbccddeeff10203040"
	crawlerSoughtNodeIDB    = "ffeeddccbbaa99887766554433221100efdfcfbf"
)

type crawlerSoughtNodeIDFixture struct {
	ID        string                      `json:"id"`
	Subsystem string                      `json:"subsystem"`
	Oracle    crawlerSoughtNodeIDOracle   `json:"oracle"`
	Input     crawlerSoughtNodeIDInput    `json:"input"`
	Expected  crawlerSoughtNodeIDExpected `json:"expected"`
}

type crawlerSoughtNodeIDOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Storage     string `json:"storage"`
	Consumers   string `json:"consumers"`
	Clock       string `json:"clock"`
	Random      string `json:"random"`
}

type crawlerSoughtNodeIDInput struct {
	Kind   string                     `json:"kind"`
	Actors []string                   `json:"actors"`
	Writes []crawlerSoughtNodeIDWrite `json:"writes"`
}

type crawlerSoughtNodeIDWrite struct {
	Actor  string `json:"actor"`
	Target string `json:"target"`
}

type crawlerSoughtNodeIDExpected struct {
	Reads       []crawlerSoughtNodeIDRead  `json:"reads"`
	Events      []string                   `json:"events"`
	FinalTarget string                     `json:"finalTarget"`
	Source      *crawlerSoughtNodeIDSource `json:"source,omitempty"`
}

type crawlerSoughtNodeIDRead struct {
	Actor  string `json:"actor"`
	After  string `json:"after"`
	Target string `json:"target"`
}

type crawlerSoughtNodeIDSource struct {
	AtomicZeroValueIsZeroID               bool              `json:"atomicZeroValueIsZeroId"`
	AtomicGetUsesReadLock                 bool              `json:"atomicGetUsesReadLock"`
	AtomicGetReturnsWholeValueCopy        bool              `json:"atomicGetReturnsWholeValueCopy"`
	AtomicSetUsesExclusiveLock            bool              `json:"atomicSetUsesExclusiveLock"`
	TargetStorageIsSharedPointer          bool              `json:"targetStorageIsSharedPointer"`
	TargetInitializedBeforeCrawlerStart   bool              `json:"targetInitializedBeforeCrawlerStart"`
	InitialTargetNonzeroGuaranteed        bool              `json:"initialTargetNonzeroGuaranteed"`
	FindReadsTargetAtEachClientCall       bool              `json:"findReadsTargetAtEachClientCall"`
	SampleReadsSameTargetAtEachClientCall bool              `json:"sampleReadsSameTargetAtEachClientCall"`
	RotationStartedAsDetachedGoroutine    bool              `json:"rotationStartedAsDetachedGoroutine"`
	RotationJoinedBeforeStartReturns      bool              `json:"rotationJoinedBeforeStartReturns"`
	RotationContextCancelledAfterStop     bool              `json:"rotationContextCancelledAfterStop"`
	RotationDelaySeconds                  int               `json:"rotationDelaySeconds"`
	RotationUsesFreshTimeAfterEachLoop    bool              `json:"rotationUsesFreshTimeAfterEachLoop"`
	RotationHasNoImmediateReplacement     bool              `json:"rotationHasNoImmediateReplacement"`
	RotationNextDelayStartsAfterSet       bool              `json:"rotationNextDelayStartsAfterSet"`
	RotationTimerBacklogPossible          bool              `json:"rotationTimerBacklogPossible"`
	RotationCancelTimerTieOutcome         string            `json:"rotationCancelTimerTieOutcome"`
	RotationRandomAndSetCancellationAware bool              `json:"rotationRandomAndSetCancellationAware"`
	RandomByteLength                      int               `json:"randomByteLength"`
	RandomSource                          string            `json:"randomSource"`
	RandomReadResultChecked               bool              `json:"randomReadResultChecked"`
	RandomAppliesClientSuffix             bool              `json:"randomAppliesClientSuffix"`
	RandomFailurePreservesPreviousTarget  bool              `json:"randomFailurePreservesPreviousTarget"`
	RandomFailureOutcome                  string            `json:"randomFailureOutcome"`
	AtomicRuntimeObserved                 bool              `json:"atomicRuntimeObserved"`
	ClockRuntimeObserved                  bool              `json:"clockRuntimeObserved"`
	RandomRuntimeObserved                 bool              `json:"randomRuntimeObserved"`
	ClockAndRandomEvidenceScope           string            `json:"clockAndRandomEvidenceScope"`
	SourceSHA256                          map[string]string `json:"sourceSha256"`
	Evidence                              string            `json:"evidence"`
}

func TestGenerateDHTCrawlerSoughtNodeIDParity(t *testing.T) {
	assertCrawlerSoughtNodeIDSourceShapes(t)
	fixtures := []crawlerSoughtNodeIDFixture{
		crawlerSoughtNodeIDSourceFixture(t),
		crawlerSoughtNodeIDZeroFixture(),
		crawlerSoughtNodeIDSetGetFixture(),
		crawlerSoughtNodeIDAliasesFixture(),
		crawlerSoughtNodeIDCrossGoroutineFixture(),
	}
	if len(fixtures) != len(crawlerSoughtNodeIDFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerSoughtNodeIDFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerSoughtNodeIDFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerSoughtNodeIDFixtureIDs[index])
		}
	}
	reconcileCrawlerSoughtNodeIDFixtures(t, fixtures)
}

func crawlerSoughtNodeIDSourceFixture(t *testing.T) crawlerSoughtNodeIDFixture {
	t.Helper()
	return crawlerSoughtNodeIDFixture{
		ID:        crawlerSoughtNodeIDFixtureIDs[0],
		Subsystem: "dht_crawler_sought_node_id",
		Oracle: crawlerSoughtNodeIDOracle{
			Composition: "production_source_and_atomic_runtime_freshness_gate",
			Determinism: "exact_go_ast_and_source_sha256_clock_and_random_source_only",
			Storage:     "production_concurrency_AtomicValue_protocol_ID",
			Consumers:   "production_find_node_and_sample_infohashes_call_sites",
			Clock:       "source_only_time_After_no_wall_clock_execution",
			Random:      "source_only_crypto_rand_no_entropy_execution",
		},
		Input: crawlerSoughtNodeIDInput{
			Kind: "source_contract", Actors: []string{}, Writes: []crawlerSoughtNodeIDWrite{},
		},
		Expected: crawlerSoughtNodeIDExpected{
			Reads: []crawlerSoughtNodeIDRead{}, Events: []string{}, FinalTarget: "",
			Source: &crawlerSoughtNodeIDSource{
				AtomicZeroValueIsZeroID:               true,
				AtomicGetUsesReadLock:                 true,
				AtomicGetReturnsWholeValueCopy:        true,
				AtomicSetUsesExclusiveLock:            true,
				TargetStorageIsSharedPointer:          true,
				TargetInitializedBeforeCrawlerStart:   true,
				InitialTargetNonzeroGuaranteed:        false,
				FindReadsTargetAtEachClientCall:       true,
				SampleReadsSameTargetAtEachClientCall: true,
				RotationStartedAsDetachedGoroutine:    true,
				RotationJoinedBeforeStartReturns:      false,
				RotationContextCancelledAfterStop:     true,
				RotationDelaySeconds:                  10,
				RotationUsesFreshTimeAfterEachLoop:    true,
				RotationHasNoImmediateReplacement:     true,
				RotationNextDelayStartsAfterSet:       true,
				RotationTimerBacklogPossible:          false,
				RotationCancelTimerTieOutcome:         "go_select_unspecified_ready_case_selection",
				RotationRandomAndSetCancellationAware: false,
				RandomByteLength:                      20,
				RandomSource:                          "crypto/rand.Read",
				RandomReadResultChecked:               false,
				RandomAppliesClientSuffix:             false,
				RandomFailurePreservesPreviousTarget:  false,
				RandomFailureOutcome:                  "ignored_error_installs_new_id_with_any_written_prefix_and_zero_initialized_remainder",
				AtomicRuntimeObserved:                 true,
				ClockRuntimeObserved:                  false,
				RandomRuntimeObserved:                 false,
				ClockAndRandomEvidenceScope:           "exact_ast_and_source_digest_only_not_runtime_executed",
				SourceSHA256:                          crawlerSoughtNodeIDSourceDigests(t),
				Evidence:                              "actual_AtomicValue_rows_plus_exact_Go_AST_and_source_freshness",
			},
		},
	}
}

func crawlerSoughtNodeIDRuntimeOracle() crawlerSoughtNodeIDOracle {
	return crawlerSoughtNodeIDOracle{
		Composition: "actual_concurrency_AtomicValue_protocol_ID",
		Determinism: "synchronous_or_channel_gated_operations",
		Storage:     "production_concurrency_AtomicValue_protocol_ID",
		Consumers:   "controlled_test_actors",
		Clock:       "not_invoked",
		Random:      "not_invoked",
	}
}

func crawlerSoughtNodeIDZeroFixture() crawlerSoughtNodeIDFixture {
	target := &concurrency.AtomicValue[protocol.ID]{}
	got := target.Get()
	return crawlerSoughtNodeIDFixture{
		ID: crawlerSoughtNodeIDFixtureIDs[1], Subsystem: "dht_crawler_sought_node_id",
		Oracle: crawlerSoughtNodeIDRuntimeOracle(),
		Input: crawlerSoughtNodeIDInput{
			Kind: "zero_get", Actors: []string{"main"}, Writes: []crawlerSoughtNodeIDWrite{},
		},
		Expected: crawlerSoughtNodeIDExpected{
			Reads:  []crawlerSoughtNodeIDRead{{Actor: "main", After: "zero_value", Target: got.String()}},
			Events: []string{"main_get:" + got.String()}, FinalTarget: got.String(),
		},
	}
}

func crawlerSoughtNodeIDSetGetFixture() crawlerSoughtNodeIDFixture {
	target := &concurrency.AtomicValue[protocol.ID]{}
	a := crawlerSoughtNodeID(crawlerSoughtNodeIDA)
	target.Set(a)
	got := target.Get()
	return crawlerSoughtNodeIDFixture{
		ID: crawlerSoughtNodeIDFixtureIDs[2], Subsystem: "dht_crawler_sought_node_id",
		Oracle: crawlerSoughtNodeIDRuntimeOracle(),
		Input: crawlerSoughtNodeIDInput{
			Kind: "set_get", Actors: []string{"main"},
			Writes: []crawlerSoughtNodeIDWrite{{Actor: "main", Target: a.String()}},
		},
		Expected: crawlerSoughtNodeIDExpected{
			Reads:  []crawlerSoughtNodeIDRead{{Actor: "main", After: "main_set_a", Target: got.String()}},
			Events: []string{"main_set:" + a.String(), "main_get:" + got.String()}, FinalTarget: got.String(),
		},
	}
}

func crawlerSoughtNodeIDAliasesFixture() crawlerSoughtNodeIDFixture {
	primary := &concurrency.AtomicValue[protocol.ID]{}
	aliasOne := primary
	aliasTwo := primary
	a := crawlerSoughtNodeID(crawlerSoughtNodeIDA)
	b := crawlerSoughtNodeID(crawlerSoughtNodeIDB)
	primary.Set(a)
	afterA := aliasOne.Get()
	aliasTwo.Set(b)
	primaryAfterB := primary.Get()
	aliasAfterB := aliasOne.Get()
	return crawlerSoughtNodeIDFixture{
		ID: crawlerSoughtNodeIDFixtureIDs[3], Subsystem: "dht_crawler_sought_node_id",
		Oracle: crawlerSoughtNodeIDRuntimeOracle(),
		Input: crawlerSoughtNodeIDInput{
			Kind: "shared_aliases", Actors: []string{"primary", "alias_one", "alias_two"},
			Writes: []crawlerSoughtNodeIDWrite{
				{Actor: "primary", Target: a.String()}, {Actor: "alias_two", Target: b.String()},
			},
		},
		Expected: crawlerSoughtNodeIDExpected{
			Reads: []crawlerSoughtNodeIDRead{
				{Actor: "alias_one", After: "primary_set_a", Target: afterA.String()},
				{Actor: "primary", After: "alias_two_set_b", Target: primaryAfterB.String()},
				{Actor: "alias_one", After: "alias_two_set_b", Target: aliasAfterB.String()},
			},
			Events: []string{
				"primary_set:" + a.String(), "alias_one_get:" + afterA.String(),
				"alias_two_set:" + b.String(), "primary_get:" + primaryAfterB.String(),
				"alias_one_get:" + aliasAfterB.String(),
			},
			FinalTarget: primaryAfterB.String(),
		},
	}
}

func crawlerSoughtNodeIDCrossGoroutineFixture() crawlerSoughtNodeIDFixture {
	target := &concurrency.AtomicValue[protocol.ID]{}
	a := crawlerSoughtNodeID(crawlerSoughtNodeIDA)
	b := crawlerSoughtNodeID(crawlerSoughtNodeIDB)
	writeRequests := make(chan protocol.ID)
	written := make(chan struct{})
	readRequests := make(chan struct{})
	readValues := make(chan protocol.ID)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for value := range writeRequests {
			target.Set(value)
			written <- struct{}{}
		}
	}()
	go func() {
		defer workers.Done()
		for range readRequests {
			readValues <- target.Get()
		}
	}()

	events := make([]string, 0, 4)
	writeRequests <- a
	<-written
	events = append(events, "writer_set:"+a.String())
	readRequests <- struct{}{}
	aRead := <-readValues
	events = append(events, "reader_get:"+aRead.String())
	writeRequests <- b
	<-written
	events = append(events, "writer_set:"+b.String())
	readRequests <- struct{}{}
	bRead := <-readValues
	events = append(events, "reader_get:"+bRead.String())
	close(writeRequests)
	close(readRequests)
	workers.Wait()
	final := target.Get()

	return crawlerSoughtNodeIDFixture{
		ID: crawlerSoughtNodeIDFixtureIDs[4], Subsystem: "dht_crawler_sought_node_id",
		Oracle: crawlerSoughtNodeIDRuntimeOracle(),
		Input: crawlerSoughtNodeIDInput{
			Kind: "controlled_cross_goroutine_handoff", Actors: []string{"writer", "reader"},
			Writes: []crawlerSoughtNodeIDWrite{
				{Actor: "writer", Target: a.String()}, {Actor: "writer", Target: b.String()},
			},
		},
		Expected: crawlerSoughtNodeIDExpected{
			Reads: []crawlerSoughtNodeIDRead{
				{Actor: "reader", After: "writer_set_a", Target: aRead.String()},
				{Actor: "reader", After: "writer_set_b", Target: bRead.String()},
			},
			Events: events, FinalTarget: final.String(),
		},
	}
}

func crawlerSoughtNodeID(value string) protocol.ID {
	return protocol.MustParseID(value)
}

func assertCrawlerSoughtNodeIDSourceShapes(t *testing.T) {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	atomicPath := filepath.Join(root, "internal/concurrency/atomic.go")
	getSet, get := crawlerPingWorkerParseFunc(t, atomicPath, "Get")
	crawlerSoughtNodeIDAssertBody(t, getSet, get, `package concurrency
func (a *AtomicValue[T]) Get() T {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	return a.value
}`)
	setSet, set := crawlerPingWorkerParseFunc(t, atomicPath, "Set")
	crawlerSoughtNodeIDAssertBody(t, setSet, set, `package concurrency
func (a *AtomicValue[T]) Set(value T) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.value = value
}`)

	crawlerPath := filepath.Join(root, "internal/dhtcrawler/crawler.go")
	crawlerSoughtNodeIDAssertCrawlerField(t, crawlerPath)
	startSet, start := crawlerPingWorkerParseFunc(t, crawlerPath, "start")
	crawlerSoughtNodeIDAssertBody(t, startSet, start, `package dhtcrawler
func (c *crawler) start() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.rotateSoughtNodeID(ctx)
	go c.runDiscoveredNodes(ctx)
	go c.runPing(ctx)
	go c.runFindNode(ctx)
	go c.getNodesForFindNode(ctx)
	go c.runSampleInfoHashes(ctx)
	go c.getNodesForSampleInfoHashes(ctx)
	go c.runInfoHashTriage(ctx)
	go c.runGetPeers(ctx)
	go c.runRequestMetaInfo(ctx)
	go c.runScrape(ctx)
	go c.reseedBootstrapNodes(ctx)
	go c.runPersistTorrents(ctx)
	go c.runPersistSources(ctx)
	go c.getOldNodes(ctx)
	<-c.stopped
}`)
	rotateSet, rotate := crawlerPingWorkerParseFunc(t, crawlerPath, "rotateSoughtNodeID")
	crawlerSoughtNodeIDAssertBody(t, rotateSet, rotate, `package dhtcrawler
func (c *crawler) rotateSoughtNodeID(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
			c.soughtNodeID.Set(protocol.RandomNodeID())
		}
	}
}`)

	crawlerSoughtNodeIDAssertFactory(t, filepath.Join(root, "internal/dhtcrawler/factory.go"))
	crawlerSoughtNodeIDAssertConsumer(t, filepath.Join(root, "internal/dhtcrawler/find_node.go"),
		"runFindNode", "c.client.FindNode(ctx, p.Addr(), c.soughtNodeID.Get())")
	crawlerSoughtNodeIDAssertConsumer(t, filepath.Join(root, "internal/dhtcrawler/sample_infohashes.go"),
		"runSampleInfoHashes", "c.client.SampleInfoHashes(ctx, n.Addr(), c.soughtNodeID.Get())")

	idPath := filepath.Join(root, "internal/protocol/id.go")
	randomSet, random := crawlerPingWorkerParseFunc(t, idPath, "RandomNodeID")
	crawlerSoughtNodeIDAssertBody(t, randomSet, random, `package protocol
func RandomNodeID() (id ID) {
	_, _ = crand.Read(id[:])
	return
}`)
	crawlerSoughtNodeIDAssertIDWidth(t, idPath)
}

func crawlerSoughtNodeIDAssertBody(
	t *testing.T,
	gotSet *token.FileSet,
	got *ast.FuncDecl,
	wantSource string,
) {
	t.Helper()
	wantSet := token.NewFileSet()
	wantFile, err := parser.ParseFile(wantSet, "expected.go", wantSource, 0)
	if err != nil {
		t.Fatal(err)
	}
	want, ok := wantFile.Decls[0].(*ast.FuncDecl)
	if !ok {
		t.Fatal("expected function declaration missing")
	}
	gotText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, gotSet, got.Body))
	wantText := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, wantSet, want.Body))
	if gotText != wantText {
		t.Fatalf("%s AST body changed\ngot: %q\nwant: %q", got.Name.Name, gotText, wantText)
	}
}

func crawlerSoughtNodeIDAssertCrawlerField(t *testing.T, path string) {
	t.Helper()
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.TYPE {
			continue
		}
		for _, spec := range generic.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "crawler" {
				continue
			}
			structure, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				t.Fatal("crawler is no longer a struct")
			}
			for _, field := range structure.Fields.List {
				if len(field.Names) == 1 && field.Names[0].Name == "soughtNodeID" {
					got := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, fileSet, field.Type))
					want := crawlerPingWorkerTokenText("*concurrency.AtomicValue[protocol.ID]")
					if got != want {
						t.Fatalf("crawler soughtNodeID type changed: %s", crawlerPingWorkerASTText(t, fileSet, field.Type))
					}
					return
				}
			}
		}
	}
	t.Fatal("crawler soughtNodeID field missing")
}

func crawlerSoughtNodeIDAssertFactory(t *testing.T, path string) {
	t.Helper()
	fileSet, factory := crawlerPingWorkerParseFunc(t, path, "New")
	var sought ast.Expr
	count := 0
	ast.Inspect(factory.Body, func(node ast.Node) bool {
		entry, ok := node.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		key, ok := entry.Key.(*ast.Ident)
		if ok && key.Name == "soughtNodeID" {
			count++
			sought = entry.Value
		}
		return true
	})
	if count != 1 {
		t.Fatalf("factory soughtNodeID initializers = %d, want 1", count)
	}
	crawlerPingWorkerAssertExpr(t, fileSet, sought, "&concurrency.AtomicValue[protocol.ID]{}")
	text := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, fileSet, factory.Body))
	setTarget := crawlerPingWorkerTokenText("c.soughtNodeID.Set(protocol.RandomNodeID())")
	start := crawlerPingWorkerTokenText("go c.start()")
	if strings.Count(text, setTarget) != 1 || strings.Count(text, start) != 1 {
		t.Fatal("factory target initialization or crawler start count changed")
	}
	if strings.Index(text, setTarget) >= strings.Index(text, start) {
		t.Fatal("factory no longer initializes the target before crawler start")
	}
}

func crawlerSoughtNodeIDAssertConsumer(t *testing.T, path, function, exactCall string) {
	t.Helper()
	fileSet, declaration := crawlerPingWorkerParseFunc(t, path, function)
	wantCall := crawlerPingWorkerTokenText(exactCall)
	wantGet := crawlerPingWorkerTokenText("c.soughtNodeID.Get()")
	callCount := 0
	getCount := 0
	ast.Inspect(declaration.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		text := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, fileSet, call))
		if text == wantCall {
			callCount++
		}
		if text == wantGet {
			getCount++
		}
		return true
	})
	if callCount != 1 || getCount != 1 {
		t.Fatalf("%s target call shape changed: client calls %d target gets %d", function, callCount, getCount)
	}
}

func crawlerSoughtNodeIDAssertIDWidth(t *testing.T, path string) {
	t.Helper()
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.TYPE {
			continue
		}
		for _, spec := range generic.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "ID" {
				continue
			}
			got := crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, fileSet, typeSpec.Type))
			want := crawlerPingWorkerTokenText("[20]byte")
			if got != want {
				t.Fatalf("protocol.ID type changed: %s", crawlerPingWorkerASTText(t, fileSet, typeSpec.Type))
			}
			return
		}
	}
	t.Fatal("protocol.ID type missing")
}

func crawlerSoughtNodeIDSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	paths := []string{
		"internal/concurrency/atomic.go",
		"internal/dhtcrawler/crawler.go",
		"internal/dhtcrawler/factory.go",
		"internal/dhtcrawler/find_node.go",
		"internal/dhtcrawler/sample_infohashes.go",
		"internal/protocol/id.go",
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

func reconcileCrawlerSoughtNodeIDFixtures(t *testing.T, fixtures []crawlerSoughtNodeIDFixture) {
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
	if crawlerSoughtNodeIDFixtureSHA256 != "" && actualHash != crawlerSoughtNodeIDFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerSoughtNodeIDFixtureSHA256)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve sought-node-ID generator source")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(source), "../../testdata/parity/dht/dht_crawler_sought_node_id.jsonl"))
	if *updateDHTCrawlerSoughtNodeIDParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-sought-node-id-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler sought-node-ID fixture is stale; rerun with -update-dht-crawler-sought-node-id-parity")
	}
}
