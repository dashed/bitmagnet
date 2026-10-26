package dhtcrawler

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	boom "github.com/tylertreat/BoomFilters"
)

var updateDHTCrawlerIgnoreHashesParity = flag.Bool(
	"update-dht-crawler-ignore-hashes-parity",
	false,
	"rewrite the Rust DHT crawler ignore-hashes parity fixture",
)

const crawlerIgnoreHashesFixtureSHA256 = "7900b4046d10037b9c7541d36d79370a92ceb3135f9c81be0adef985ac1f4621"

const (
	crawlerIgnoreHashesCells             = uint(10_000_000)
	crawlerIgnoreHashesBitsPerCell       = uint8(2)
	crawlerIgnoreHashesFalsePositiveRate = 0.001
	crawlerIgnoreHashesModulePath        = "github.com/tylertreat/BoomFilters"
	crawlerIgnoreHashesModuleVersion     = "v0.0.0-20210315201527-1a82519a3e43"
	crawlerIgnoreHashesModuleSum         = "h1:QEePdg0ty2r0t1+qwfZmQ4OOl/MB2UXIeJSpIZv56lg="
	crawlerIgnoreHashesModuleGoModSum    = "h1:OYRfF6eb5wY9VRFkXJH8FFBi3plw2v+giaIu7P054pM="
	crawlerIgnoreHashesDerivedK          = uint(5)
	crawlerIgnoreHashesDerivedP          = uint(49)
	crawlerIgnoreHashesDerivedMax        = uint8(3)
	crawlerIgnoreHashesIndexBufferLength = 5
	crawlerIgnoreHashesCellPayloadBytes  = uint(2_500_000)
	crawlerIgnoreHashesSerializedBytes   = int64(2_500_091)
	crawlerIgnoreHashesContentionCalls   = 8
)

var crawlerIgnoreHashesFixtureIDs = [...]string{
	"production_source_filter_and_probabilistic_scope_contract",
	"fresh_production_filter_adjacent_duplicates",
}

type crawlerIgnoreHashesFixture struct {
	ID             string                      `json:"id"`
	Subsystem      string                      `json:"subsystem"`
	Classification string                      `json:"classification"`
	Oracle         crawlerIgnoreHashesOracle   `json:"oracle"`
	Input          crawlerIgnoreHashesInput    `json:"input"`
	Expected       crawlerIgnoreHashesExpected `json:"expected"`
}

type crawlerIgnoreHashesOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Filter      string `json:"filter"`
	Randomness  string `json:"randomness"`
}

type crawlerIgnoreHashesInput struct {
	Kind       string                                  `json:"kind"`
	Operations []crawlerIgnoreHashesOperation          `json:"operations"`
	Contention *crawlerIgnoreHashesContentionOperation `json:"contention,omitempty"`
}

type crawlerIgnoreHashesOperation struct {
	Token string `json:"token"`
	ID    string `json:"id"`
}

type crawlerIgnoreHashesContentionOperation struct {
	ID        string `json:"id"`
	CallCount int    `json:"callCount"`
}

type crawlerIgnoreHashesExpected struct {
	Results    []crawlerIgnoreHashesResult    `json:"results"`
	Contention *crawlerIgnoreHashesContention `json:"contention,omitempty"`
	Source     *crawlerIgnoreHashesSource     `json:"source,omitempty"`
}

type crawlerIgnoreHashesResult struct {
	Token          string `json:"token"`
	AlreadyPresent bool   `json:"alreadyPresent"`
}

type crawlerIgnoreHashesContention struct {
	FalseCount               int  `json:"falseCount"`
	TrueCount                int  `json:"trueCount"`
	SequentialAlreadyPresent bool `json:"sequentialAlreadyPresent"`
}

type crawlerIgnoreHashesSource struct {
	MutexCoversTestAndAdd       bool              `json:"mutexCoversTestAndAdd"`
	InputByteLength             int               `json:"inputByteLength"`
	InputProjection             string            `json:"inputProjection"`
	TestPrecedesRandomDecrement bool              `json:"testPrecedesRandomDecrement"`
	EveryCallAdds               bool              `json:"everyCallAdds"`
	ProcessLocal                bool              `json:"processLocal"`
	Persisted                   bool              `json:"persisted"`
	Cells                       uint              `json:"cells"`
	BitsPerCell                 uint8             `json:"bitsPerCell"`
	TargetFalsePositiveRate     float64           `json:"targetFalsePositiveRate"`
	DerivedHashFunctions        uint              `json:"derivedHashFunctions"`
	DerivedDecrementCells       uint              `json:"derivedDecrementCells"`
	DerivedMaxCellValue         uint8             `json:"derivedMaxCellValue"`
	DerivedIndexBufferLength    int               `json:"derivedIndexBufferLength"`
	DerivedCellPayloadBytes     uint              `json:"derivedCellPayloadBytes"`
	DerivedSerializedBytes      int64             `json:"derivedSerializedBytes"`
	HashKernel                  string            `json:"hashKernel"`
	RandomDecrementSource       string            `json:"randomDecrementSource"`
	StableEviction              bool              `json:"stableEviction"`
	FalsePositivesPossible      bool              `json:"falsePositivesPossible"`
	FalseNegativesPossible      bool              `json:"falseNegativesPossible"`
	ModulePath                  string            `json:"modulePath"`
	ModuleVersion               string            `json:"moduleVersion"`
	ModuleSourceSum             string            `json:"moduleSourceSum"`
	ModuleGoModSum              string            `json:"moduleGoModSum"`
	DependencySourcePin         string            `json:"dependencySourcePin"`
	DependencySourceVendored    bool              `json:"dependencySourceVendored"`
	GoModRequirement            string            `json:"goModRequirement"`
	GoSumModuleLine             string            `json:"goSumModuleLine"`
	GoSumGoModLine              string            `json:"goSumGoModLine"`
	AdjacentDuplicateScope      string            `json:"adjacentDuplicateScope"`
	SourceSHA256                map[string]string `json:"sourceSha256"`
	Nonclaims                   []string          `json:"nonclaims"`
	Evidence                    string            `json:"evidence"`
}

func TestGenerateDHTCrawlerIgnoreHashesParity(t *testing.T) {
	fixtures := []crawlerIgnoreHashesFixture{
		crawlerIgnoreHashesSourceFixture(t),
		crawlerIgnoreHashesRuntimeFixture(t),
	}
	if len(fixtures) != len(crawlerIgnoreHashesFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerIgnoreHashesFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerIgnoreHashesFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q, want %q", index, fixture.ID, crawlerIgnoreHashesFixtureIDs[index])
		}
	}
	reconcileCrawlerIgnoreHashesFixtures(t, fixtures)
}

func crawlerIgnoreHashesSourceFixture(t *testing.T) crawlerIgnoreHashesFixture {
	t.Helper()
	moduleDir := assertCrawlerIgnoreHashesModulePin(t)
	assertCrawlerIgnoreHashesSourceShapes(t, moduleDir)
	filter := boom.NewStableBloomFilter(
		crawlerIgnoreHashesCells,
		crawlerIgnoreHashesBitsPerCell,
		crawlerIgnoreHashesFalsePositiveRate,
	)
	serializedBytes, err := filter.WriteTo(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if filter.Cells() != crawlerIgnoreHashesCells ||
		filter.K() != crawlerIgnoreHashesDerivedK ||
		filter.P() != crawlerIgnoreHashesDerivedP ||
		uint8((1<<crawlerIgnoreHashesBitsPerCell)-1) != crawlerIgnoreHashesDerivedMax ||
		(crawlerIgnoreHashesCells*uint(crawlerIgnoreHashesBitsPerCell)+7)/8 != crawlerIgnoreHashesCellPayloadBytes ||
		serializedBytes != crawlerIgnoreHashesSerializedBytes {
		t.Fatalf(
			"production filter derivation = cells:%d k:%d p:%d max:%d payload:%d serialized:%d",
			filter.Cells(), filter.K(), filter.P(),
			uint8((1<<crawlerIgnoreHashesBitsPerCell)-1),
			(crawlerIgnoreHashesCells*uint(crawlerIgnoreHashesBitsPerCell)+7)/8,
			serializedBytes,
		)
	}
	goModRequirement := crawlerIgnoreHashesModulePath + " " + crawlerIgnoreHashesModuleVersion
	goSumModuleLine := goModRequirement + " " + crawlerIgnoreHashesModuleSum
	goSumGoModLine := goModRequirement + "/go.mod " + crawlerIgnoreHashesModuleGoModSum
	return crawlerIgnoreHashesFixture{
		ID:             crawlerIgnoreHashesFixtureIDs[0],
		Subsystem:      "dht_crawler_ignore_hashes",
		Classification: "SOURCE_ONLY",
		Oracle: crawlerIgnoreHashesOracle{
			Composition: "exact_production_wrapper_factory_and_module_source_pin",
			Determinism: "normalized_AST_exact_repo_and_module_source_SHA256_and_Go_module_lines",
			Filter:      "BoomFilters_StableBloomFilter",
			Randomness:  "source_only_random_decrement_offsets_and_long_run_probabilistic_behavior",
		},
		Input: crawlerIgnoreHashesInput{
			Kind:       "source_contract",
			Operations: []crawlerIgnoreHashesOperation{},
		},
		Expected: crawlerIgnoreHashesExpected{
			Results: []crawlerIgnoreHashesResult{},
			Source: &crawlerIgnoreHashesSource{
				MutexCoversTestAndAdd:       true,
				InputByteLength:             len(protocol.ID{}),
				InputProjection:             "full_protocol_ID_20_byte_slice",
				TestPrecedesRandomDecrement: true,
				EveryCallAdds:               true,
				ProcessLocal:                true,
				Persisted:                   false,
				Cells:                       filter.Cells(),
				BitsPerCell:                 crawlerIgnoreHashesBitsPerCell,
				TargetFalsePositiveRate:     crawlerIgnoreHashesFalsePositiveRate,
				DerivedHashFunctions:        crawlerIgnoreHashesDerivedK,
				DerivedDecrementCells:       crawlerIgnoreHashesDerivedP,
				DerivedMaxCellValue:         crawlerIgnoreHashesDerivedMax,
				DerivedIndexBufferLength:    crawlerIgnoreHashesIndexBufferLength,
				DerivedCellPayloadBytes:     crawlerIgnoreHashesCellPayloadBytes,
				DerivedSerializedBytes:      crawlerIgnoreHashesSerializedBytes,
				HashKernel:                  "FNV-1_64; index_i=(low32(sum)+high32(sum)*i)%10_000_000",
				RandomDecrementSource:       "one_math/rand_Intn(10_000_000)_start_then_49_adjacent_cells_modulo_10_000_000",
				StableEviction:              true,
				FalsePositivesPossible:      true,
				FalseNegativesPossible:      true,
				ModulePath:                  crawlerIgnoreHashesModulePath,
				ModuleVersion:               crawlerIgnoreHashesModuleVersion,
				ModuleSourceSum:             crawlerIgnoreHashesModuleSum,
				ModuleGoModSum:              crawlerIgnoreHashesModuleGoModSum,
				DependencySourcePin:         "Go_module_zip_h1_sum",
				DependencySourceVendored:    false,
				GoModRequirement:            goModRequirement,
				GoSumModuleLine:             goSumModuleLine,
				GoSumGoModLine:              goSumGoModLine,
				AdjacentDuplicateScope:      "fresh_zero_filter_two_distinct_IDs_each_immediately_repeated",
				SourceSHA256:                crawlerIgnoreHashesSourceDigests(t, moduleDir),
				Nonclaims: []string{
					"exact_random_decrement_offsets_or_cells",
					"exact_math_rand_seed_or_sequence",
					"exact_set_membership_semantics",
					"measured_or_guaranteed_false_positive_or_false_negative_rates",
					"long_run_false_positive_sequence",
					"long_run_false_negative_sequence",
					"exact_eviction_age_or_retention_window",
					"cross_goroutine_winner_or_completion_order",
					"mutex_lock_fairness",
					"mutex_lock_throughput",
					"packed_cell_payload_as_total_heap_or_allocator_footprint",
					"serialized_filter_contents",
					"process_restart_persistence",
					"Rust_implementation_or_public_API",
					"sample_infohashes_worker_end_to_end_behavior",
					"query_triage_KTable_recursive_fanout_supervisor_or_live_behavior",
				},
				Evidence: "the runtime row calls the actual mutex wrapper over one fresh production-parameter filter; only adjacent-duplicate results and contention aggregates are exact",
			},
		},
	}
}

func crawlerIgnoreHashesRuntimeFixture(t *testing.T) crawlerIgnoreHashesFixture {
	t.Helper()
	operations := []crawlerIgnoreHashesOperation{
		{Token: "A:first", ID: "00000000000000000000000000000000000000a1"},
		{Token: "A:adjacent_duplicate", ID: "00000000000000000000000000000000000000a1"},
		{Token: "B:first", ID: "00000000000000000000000000000000000000b2"},
		{Token: "B:adjacent_duplicate", ID: "00000000000000000000000000000000000000b2"},
	}
	filter := &ignoreHashes{bloom: boom.NewStableBloomFilter(
		crawlerIgnoreHashesCells,
		crawlerIgnoreHashesBitsPerCell,
		crawlerIgnoreHashesFalsePositiveRate,
	)}
	results := make([]crawlerIgnoreHashesResult, 0, len(operations))
	for _, operation := range operations {
		id := protocol.MustParseID(operation.ID)
		results = append(results, crawlerIgnoreHashesResult{
			Token: operation.Token, AlreadyPresent: filter.testAndAdd(id),
		})
	}
	want := []bool{false, true, false, true}
	for index, result := range results {
		if result.AlreadyPresent != want[index] {
			t.Fatalf("operation %s membership = %t, want %t", result.Token, result.AlreadyPresent, want[index])
		}
	}
	contentionInput := crawlerIgnoreHashesContentionOperation{
		ID: "00000000000000000000000000000000000000c3", CallCount: crawlerIgnoreHashesContentionCalls,
	}
	contentionID := protocol.MustParseID(contentionInput.ID)
	start := make(chan struct{})
	contentionResults := make(chan bool, contentionInput.CallCount)
	for range contentionInput.CallCount {
		go func() {
			<-start
			contentionResults <- filter.testAndAdd(contentionID)
		}()
	}
	close(start)
	contention := crawlerIgnoreHashesContention{}
	for range contentionInput.CallCount {
		if <-contentionResults {
			contention.TrueCount++
		} else {
			contention.FalseCount++
		}
	}
	contention.SequentialAlreadyPresent = filter.testAndAdd(contentionID)
	if contention.FalseCount != 1 ||
		contention.TrueCount != contentionInput.CallCount-1 ||
		!contention.SequentialAlreadyPresent {
		t.Fatalf("contention result = %+v, want one miss then only hits in aggregate", contention)
	}
	return crawlerIgnoreHashesFixture{
		ID:             crawlerIgnoreHashesFixtureIDs[1],
		Subsystem:      "dht_crawler_ignore_hashes",
		Classification: "RUNTIME_EXACT",
		Oracle: crawlerIgnoreHashesOracle{
			Composition: "actual_ignoreHashes_testAndAdd_with_fresh_production_BoomFilters_instance",
			Determinism: "fresh_zero_filter_adjacent_duplicates_and_same_ID_contention_aggregate_only",
			Filter:      "actual_BoomFilters_StableBloomFilter",
			Randomness:  "random_decrement_does_not_change_this_membership_prefix",
		},
		Input: crawlerIgnoreHashesInput{
			Kind: "actual_fresh_production_ignore_hashes", Operations: operations, Contention: &contentionInput,
		},
		Expected: crawlerIgnoreHashesExpected{Results: results, Contention: &contention},
	}
}

func assertCrawlerIgnoreHashesSourceShapes(t *testing.T, moduleDir string) {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	crawlerPath := filepath.Join(root, "internal/dhtcrawler/crawler.go")
	crawlerSet, method := crawlerPingWorkerParseFunc(t, crawlerPath, "testAndAdd")
	wantSet, want := crawlerFindNodeWorkerParseSourceFunc(t, `package dhtcrawler
func (i *ignoreHashes) testAndAdd(id protocol.ID) bool {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	return i.bloom.TestAndAdd(id[:])
}`)
	crawlerFindNodeWorkerAssertBody(t, crawlerSet, method, wantSet, want, "ignoreHashes.testAndAdd")

	contents, err := os.ReadFile(crawlerPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"type ignoreHashes struct {",
		"mutex sync.Mutex",
		"bloom *boom.StableBloomFilter",
	} {
		if !strings.Contains(string(contents), required) {
			t.Fatalf("crawler ignoreHashes source missing %q", required)
		}
	}

	factorySet, factory := crawlerPingWorkerParseFunc(
		t,
		filepath.Join(root, "internal/dhtcrawler/factory.go"),
		"New",
	)
	var ignoreHashesExpr ast.Expr
	ast.Inspect(factory.Body, func(node ast.Node) bool {
		entry, ok := node.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		key, ok := entry.Key.(*ast.Ident)
		if ok && key.Name == "ignoreHashes" {
			ignoreHashesExpr = entry.Value
		}
		return true
	})
	crawlerPingWorkerAssertExpr(t, factorySet, ignoreHashesExpr, `&ignoreHashes{
		bloom: boom.NewStableBloomFilter(10_000_000, 2, 0.001),
	}`)

	samplePath := filepath.Join(root, "internal/dhtcrawler/sample_infohashes.go")
	sampleSet, sample := crawlerPingWorkerParseFunc(t, samplePath, "runSampleInfoHashes")
	var sampleRange *ast.RangeStmt
	ast.Inspect(sample.Body, func(node ast.Node) bool {
		rangeStatement, ok := node.(*ast.RangeStmt)
		if ok && crawlerPingWorkerASTText(t, sampleSet, rangeStatement.X) == "res.Samples" {
			sampleRange = rangeStatement
		}
		return true
	})
	wantSampleSet, wantSample := crawlerFindNodeWorkerParseSourceFunc(t, `package dhtcrawler
func sampleCallsite() {
	for _, s := range res.Samples {
		if !c.ignoreHashes.testAndAdd(s) {
			discoveredHashes = append(discoveredHashes, nodeHasPeersForHash{
				infoHash: s,
				node:     n.Addr(),
			})
		}
	}
}`)
	wantSampleRange := wantSample.Body.List[0].(*ast.RangeStmt)
	if sampleRange == nil ||
		crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, sampleSet, sampleRange)) !=
			crawlerPingWorkerTokenText(crawlerPingWorkerASTText(t, wantSampleSet, wantSampleRange)) {
		t.Fatal("sample_infohashes ignoreHashes ordered callsite AST changed")
	}

	boomSet, hashKernel := crawlerPingWorkerParseFunc(t, filepath.Join(moduleDir, "boom.go"), "hashKernel")
	optimalKSet, optimalK := crawlerPingWorkerParseFunc(t, filepath.Join(moduleDir, "boom.go"), "OptimalK")
	wantOptimalKSet, wantOptimalK := crawlerFindNodeWorkerParseSourceFunc(t, `package boom
func OptimalK(fpRate float64) uint {
	return uint(math.Ceil(math.Log2(1 / fpRate)))
}`)
	crawlerFindNodeWorkerAssertBody(t, optimalKSet, optimalK, wantOptimalKSet, wantOptimalK, "boom.OptimalK")

	wantBoomSet, wantHashKernel := crawlerFindNodeWorkerParseSourceFunc(t, `package boom
func hashKernel(data []byte, hash hash.Hash64) (uint32, uint32) {
	hash.Write(data)
	sum := hash.Sum64()
	hash.Reset()
	upper := uint32(sum & 0xffffffff)
	lower := uint32((sum >> 32) & 0xffffffff)
	return upper, lower
}`)
	crawlerFindNodeWorkerAssertBody(t, boomSet, hashKernel, wantBoomSet, wantHashKernel, "boom.hashKernel")

	stablePath := filepath.Join(moduleDir, "stable.go")
	stableSet, constructor := crawlerPingWorkerParseFunc(t, stablePath, "NewStableBloomFilter")
	wantConstructorSet, wantConstructor := crawlerFindNodeWorkerParseSourceFunc(t, `package boom
func NewStableBloomFilter(m uint, d uint8, fpRate float64) *StableBloomFilter {
	k := OptimalK(fpRate) / 2
	if k > m {
		k = m
	} else if k <= 0 {
		k = 1
	}

	cells := NewBuckets(m, d)

	return &StableBloomFilter{
		hash:        fnv.New64(),
		m:           m,
		k:           k,
		p:           optimalStableP(m, k, d, fpRate),
		max:         cells.MaxBucketValue(),
		cells:       cells,
		indexBuffer: make([]uint, k),
	}
}`)
	crawlerFindNodeWorkerAssertBody(
		t,
		stableSet,
		constructor,
		wantConstructorSet,
		wantConstructor,
		"boom.NewStableBloomFilter",
	)

	optimalPSet, optimalP := crawlerPingWorkerParseFunc(t, stablePath, "optimalStableP")
	wantOptimalPSet, wantOptimalP := crawlerFindNodeWorkerParseSourceFunc(t, `package boom
func optimalStableP(m, k uint, d uint8, fpRate float64) uint {
	var (
		max      = math.Pow(2, float64(d)) - 1
		subDenom = math.Pow(1-math.Pow(fpRate, 1/float64(k)), 1/max)
		denom    = (1/subDenom - 1) * (1/float64(k) - 1/float64(m))
	)

	p := uint(1 / denom)
	if p <= 0 {
		p = 1
	}

	return p
}`)
	crawlerFindNodeWorkerAssertBody(t, optimalPSet, optimalP, wantOptimalPSet, wantOptimalP, "boom.optimalStableP")

	testAndAddSet, testAndAdd := crawlerPingWorkerParseFunc(t, stablePath, "TestAndAdd")
	wantTestSet, wantTestAndAdd := crawlerFindNodeWorkerParseSourceFunc(t, `package boom
func (s *StableBloomFilter) TestAndAdd(data []byte) bool {
	lower, upper := hashKernel(data, s.hash)
	member := true
	for i := uint(0); i < s.k; i++ {
		s.indexBuffer[i] = (uint(lower) + uint(upper)*i) % s.m
		if s.cells.Get(s.indexBuffer[i]) == 0 {
			member = false
		}
	}
	s.decrement()
	for _, idx := range s.indexBuffer {
		s.cells.Set(idx, s.max)
	}
	return member
}`)
	crawlerFindNodeWorkerAssertBody(t, testAndAddSet, testAndAdd, wantTestSet, wantTestAndAdd, "boom.StableBloomFilter.TestAndAdd")

	decrementSet, decrement := crawlerPingWorkerParseFunc(t, stablePath, "decrement")
	wantDecrementSet, wantDecrement := crawlerFindNodeWorkerParseSourceFunc(t, `package boom
func (s *StableBloomFilter) decrement() {
	r := rand.Intn(int(s.m))
	for i := uint(0); i < s.p; i++ {
		idx := (r + int(i)) % int(s.m)
		s.cells.Increment(uint(idx), -1)
	}
}`)
	crawlerFindNodeWorkerAssertBody(t, decrementSet, decrement, wantDecrementSet, wantDecrement, "boom.StableBloomFilter.decrement")

	bucketsPath := filepath.Join(moduleDir, "buckets.go")
	bucketsSet, newBuckets := crawlerPingWorkerParseFunc(t, bucketsPath, "NewBuckets")
	wantBucketsSet, wantNewBuckets := crawlerFindNodeWorkerParseSourceFunc(t, `package boom
func NewBuckets(count uint, bucketSize uint8) *Buckets {
	return &Buckets{
		count:      count,
		data:       make([]byte, (count*uint(bucketSize)+7)/8),
		bucketSize: bucketSize,
		max:        (1 << bucketSize) - 1,
	}
}`)
	crawlerFindNodeWorkerAssertBody(t, bucketsSet, newBuckets, wantBucketsSet, wantNewBuckets, "boom.NewBuckets")

	incrementSet, increment := crawlerPingWorkerParseFunc(t, bucketsPath, "Increment")
	wantIncrementSet, wantIncrement := crawlerFindNodeWorkerParseSourceFunc(t, `package boom
func (b *Buckets) Increment(bucket uint, delta int32) *Buckets {
	val := int32(b.getBits(bucket*uint(b.bucketSize), uint(b.bucketSize))) + delta
	if val > int32(b.max) {
		val = int32(b.max)
	} else if val < 0 {
		val = 0
	}

	b.setBits(uint32(bucket)*uint32(b.bucketSize), uint32(b.bucketSize), uint32(val))
	return b
}`)
	crawlerFindNodeWorkerAssertBody(t, incrementSet, increment, wantIncrementSet, wantIncrement, "boom.Buckets.Increment")
}

func assertCrawlerIgnoreHashesModulePin(t *testing.T) string {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	type moduleMetadata struct {
		Path     string          `json:"Path"`
		Version  string          `json:"Version"`
		Sum      string          `json:"Sum"`
		GoModSum string          `json:"GoModSum"`
		Dir      string          `json:"Dir"`
		Replace  *moduleMetadata `json:"Replace"`
	}
	command := exec.Command("go", "list", "-m", "-json", crawlerIgnoreHashesModulePath)
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatalf("resolve BoomFilters module: %v", err)
	}
	var module moduleMetadata
	if err := json.Unmarshal(output, &module); err != nil {
		t.Fatal(err)
	}
	if module.Path != crawlerIgnoreHashesModulePath ||
		module.Version != crawlerIgnoreHashesModuleVersion ||
		module.Sum != crawlerIgnoreHashesModuleSum ||
		module.GoModSum != crawlerIgnoreHashesModuleGoModSum ||
		module.Dir == "" ||
		module.Replace != nil {
		t.Fatalf("BoomFilters module metadata = %+v", module)
	}
	goMod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	wantRequirement := "\t" + crawlerIgnoreHashesModulePath + " " + crawlerIgnoreHashesModuleVersion
	if !strings.Contains(string(goMod), wantRequirement+"\n") {
		t.Fatalf("go.mod missing %q", wantRequirement)
	}
	goSum, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	wantModuleLine := crawlerIgnoreHashesModulePath + " " + crawlerIgnoreHashesModuleVersion + " " + crawlerIgnoreHashesModuleSum
	wantGoModLine := crawlerIgnoreHashesModulePath + " " + crawlerIgnoreHashesModuleVersion + "/go.mod " + crawlerIgnoreHashesModuleGoModSum
	for _, line := range []string{wantModuleLine, wantGoModLine} {
		if !strings.Contains(string(goSum), line+"\n") {
			t.Fatalf("go.sum missing %q", line)
		}
	}
	return module.Dir
}

func crawlerIgnoreHashesSourceDigests(t *testing.T, moduleDir string) map[string]string {
	t.Helper()
	root := crawlerPingWorkerRoot(t)
	paths := map[string]string{
		"internal/dhtcrawler/crawler.go":           filepath.Join(root, "internal/dhtcrawler/crawler.go"),
		"internal/dhtcrawler/factory.go":           filepath.Join(root, "internal/dhtcrawler/factory.go"),
		"internal/dhtcrawler/sample_infohashes.go": filepath.Join(root, "internal/dhtcrawler/sample_infohashes.go"),
		"internal/protocol/id.go":                  filepath.Join(root, "internal/protocol/id.go"),
		crawlerIgnoreHashesModulePath + "@" + crawlerIgnoreHashesModuleVersion + "/boom.go":    filepath.Join(moduleDir, "boom.go"),
		crawlerIgnoreHashesModulePath + "@" + crawlerIgnoreHashesModuleVersion + "/buckets.go": filepath.Join(moduleDir, "buckets.go"),
		crawlerIgnoreHashesModulePath + "@" + crawlerIgnoreHashesModuleVersion + "/stable.go":  filepath.Join(moduleDir, "stable.go"),
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

func reconcileCrawlerIgnoreHashesFixtures(t *testing.T, fixtures []crawlerIgnoreHashesFixture) {
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
	if crawlerIgnoreHashesFixtureSHA256 != "" && actualHash != crawlerIgnoreHashesFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerIgnoreHashesFixtureSHA256)
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve ignore-hashes generator source")
	}
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source),
		"../../testdata/parity/dht/dht_crawler_ignore_hashes.jsonl",
	))
	if *updateDHTCrawlerIgnoreHashesParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-ignore-hashes-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler ignore-hashes fixture is stale; rerun with -update-dht-crawler-ignore-hashes-parity")
	}
}
