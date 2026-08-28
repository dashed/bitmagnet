package dhtcrawler

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

var updateDHTCrawlerCompositionParity = flag.Bool(
	"update-dht-crawler-composition-parity",
	false,
	"rewrite the Rust DHT crawler composition source fixture",
)

const crawlerCompositionFixtureSHA256 = "fc1dfd4e28f0cd32aeef424af5f4b8aa65f18c0a663589e31daa174010bb0474"

const crawlerCompositionFixtureID = "production_construction_and_lifecycle_source_contract"

type crawlerCompositionFixture struct {
	ID             string                     `json:"id"`
	Subsystem      string                     `json:"subsystem"`
	Classification string                     `json:"classification"`
	Execution      string                     `json:"execution"`
	Oracle         crawlerCompositionOracle   `json:"oracle"`
	Input          crawlerCompositionInput    `json:"input"`
	Expected       crawlerCompositionExpected `json:"expected"`
}

type crawlerCompositionOracle struct {
	Composition                 string   `json:"composition"`
	Determinism                 string   `json:"determinism"`
	ActualFunctionsExecuted     []string `json:"actualFunctionsExecuted"`
	SourcePinnedFunctions       []string `json:"sourcePinnedFunctions"`
	ProductionFunctionsExecuted bool     `json:"productionFunctionsExecuted"`
	NetworkExecuted             bool     `json:"networkExecuted"`
	DatabaseExecuted            bool     `json:"databaseExecuted"`
	GoroutinesStarted           bool     `json:"goroutinesStarted"`
}

type crawlerCompositionInput struct {
	Kind string `json:"kind"`
}

type crawlerCompositionExpected struct {
	Config                    crawlerCompositionConfig      `json:"config"`
	Scaling                   crawlerCompositionScaling     `json:"scaling"`
	Routes                    []crawlerCompositionRoute     `json:"routes"`
	CrawlerLaunches           []crawlerCompositionLaunch    `json:"crawlerLaunches"`
	Lifecycle                 crawlerCompositionLifecycle   `json:"lifecycle"`
	Requester                 crawlerCompositionRequester   `json:"requester"`
	Blocking                  crawlerCompositionBlocking    `json:"blocking"`
	Persistence               crawlerCompositionPersistence `json:"persistence"`
	NormalizedASTSHA256       map[string]string             `json:"normalizedAstSha256"`
	SourceSHA256              map[string]string             `json:"sourceSha256"`
	PrerequisiteFixtureSHA256 map[string]string             `json:"prerequisiteFixtureSha256"`
	Nonclaims                 []string                      `json:"nonclaims"`
}

type crawlerCompositionConfig struct {
	ScalingFactor                        uint64 `json:"scalingFactor"`
	DefaultBootstrapReseedSeconds        int64  `json:"defaultBootstrapReseedSeconds"`
	FactoryBootstrapReseedSeconds        int64  `json:"factoryBootstrapReseedSeconds"`
	FactoryUsesConfiguredBootstrapReseed bool   `json:"factoryUsesConfiguredBootstrapReseed"`
	OldestNodeScanSeconds                int64  `json:"oldestNodeScanSeconds"`
	OldPeerThresholdSeconds              int64  `json:"oldPeerThresholdSeconds"`
	SaveFilesThreshold                   uint64 `json:"saveFilesThreshold"`
	SavePieces                           bool   `json:"savePieces"`
	RescrapeThresholdSeconds             int64  `json:"rescrapeThresholdSeconds"`
	SoughtNodeRotationSeconds            int64  `json:"soughtNodeRotationSeconds"`
}

type crawlerCompositionScaling struct {
	Arithmetic string                             `json:"arithmetic"`
	Formulas   []crawlerCompositionScalingFormula `json:"formulas"`
	Vectors    []crawlerCompositionScalingVector  `json:"vectors"`
}

type crawlerCompositionScalingFormula struct {
	Name                  string `json:"name"`
	CapacityExpression    string `json:"capacityExpression"`
	ConcurrencyExpression string `json:"concurrencyExpression"`
}

type crawlerCompositionScalingVector struct {
	ScalingFactor               uint64 `json:"scalingFactor"`
	DiscoveryCapacity           int64  `json:"discoveryCapacity"`
	PingCapacity                int64  `json:"pingCapacity"`
	PingConcurrency             int64  `json:"pingConcurrency"`
	FindNodeCapacity            int64  `json:"findNodeCapacity"`
	FindNodeConcurrency         int64  `json:"findNodeConcurrency"`
	SampleInfohashesCapacity    int64  `json:"sampleInfohashesCapacity"`
	SampleInfohashesConcurrency int64  `json:"sampleInfohashesConcurrency"`
	InfoHashTriageCapacity      int64  `json:"infoHashTriageCapacity"`
	GetPeersCapacity            int64  `json:"getPeersCapacity"`
	GetPeersConcurrency         int64  `json:"getPeersConcurrency"`
	ScrapeCapacity              int64  `json:"scrapeCapacity"`
	ScrapeConcurrency           int64  `json:"scrapeConcurrency"`
	RequestMetaInfoCapacity     int64  `json:"requestMetaInfoCapacity"`
	RequestMetaInfoConcurrency  int64  `json:"requestMetaInfoConcurrency"`
}

type crawlerCompositionRoute struct {
	Name                      string `json:"name"`
	Capacity                  uint64 `json:"capacity"`
	BatchSize                 uint64 `json:"batchSize"`
	BatchIntervalMilliseconds int64  `json:"batchIntervalMilliseconds"`
	Concurrency               uint64 `json:"concurrency"`
	Implementation            string `json:"implementation"`
}

type crawlerCompositionLaunch struct {
	SourceOrder int    `json:"sourceOrder"`
	Function    string `json:"function"`
	Detached    bool   `json:"detached"`
	Joined      bool   `json:"joined"`
}

type crawlerCompositionLifecycle struct {
	StartUsesBackgroundContext          bool   `json:"startUsesBackgroundContext"`
	CancelDeferredUntilStartReturns     bool   `json:"cancelDeferredUntilStartReturns"`
	StartWaitsOnlyForStopped            bool   `json:"startWaitsOnlyForStopped"`
	OnStopSetsActiveFalse               bool   `json:"onStopSetsActiveFalse"`
	OnStopClosesStopped                 bool   `json:"onStopClosesStopped"`
	OnStopJoinsCrawlerChildren          bool   `json:"onStopJoinsCrawlerChildren"`
	OnStopClosesPipelineInputs          bool   `json:"onStopClosesPipelineInputs"`
	BatchersStartDetachedAtConstruction bool   `json:"batchersStartDetachedAtConstruction"`
	BatcherOutputCapacity               uint64 `json:"batcherOutputCapacity"`
	ConcurrentCallbacksStartDetached    bool   `json:"concurrentCallbacksStartDetached"`
	ConcurrentCallbacksJoined           bool   `json:"concurrentCallbacksJoined"`
	LaunchOrderEvidence                 string `json:"launchOrderEvidence"`
}

type crawlerCompositionRequester struct {
	PeerIDGeneratedOncePerFactory       bool     `json:"peerIdGeneratedOncePerFactory"`
	PeerIDGenerator                     string   `json:"peerIdGenerator"`
	PeerIDClientPrefix                  string   `json:"peerIdClientPrefix"`
	PeerIDSeparateFromDHTNodeID         bool     `json:"peerIdSeparateFromDhtNodeId"`
	RequestTimeoutSeconds               int64    `json:"requestTimeoutSeconds"`
	ConnectTimeoutSeconds               int64    `json:"connectTimeoutSeconds"`
	WrapperCallOrder                    []string `json:"wrapperCallOrder"`
	LimiterKey                          string   `json:"limiterKey"`
	LimiterTokenIntervalMilliseconds    int64    `json:"limiterTokenIntervalMilliseconds"`
	LimiterBurst                        uint64   `json:"limiterBurst"`
	LimiterKeyCapacity                  uint64   `json:"limiterKeyCapacity"`
	LimiterTTLSeconds                   int64    `json:"limiterTtlSeconds"`
	ConfiguredKeyMutexSizeUsedByFactory bool     `json:"configuredKeyMutexSizeUsedByFactory"`
	LoggerSampleTickSeconds             int64    `json:"loggerSampleTickSeconds"`
	LoggerSampleInitial                 uint64   `json:"loggerSampleInitial"`
	LoggerSampleThereafter              uint64   `json:"loggerSampleThereafter"`
	LimiterFailuresReachLoggerOrMetrics bool     `json:"limiterFailuresReachLoggerOrMetrics"`
}

type crawlerCompositionBlocking struct {
	SharedByTriageAndRequestMetaInfo bool   `json:"sharedByTriageAndRequestMetaInfo"`
	TriageOperation                  string `json:"triageOperation"`
	BannedOperation                  string `json:"bannedOperation"`
	BannedFlushArgument              bool   `json:"bannedFlushArgument"`
	CrawlerOnStopCallsFlush          bool   `json:"crawlerOnStopCallsFlush"`
	FactoryProvidesFlushHook         bool   `json:"factoryProvidesFlushHook"`
	HookCallsIfInitialized           bool   `json:"hookCallsIfInitialized"`
	HookReturnsManagerFlushResult    bool   `json:"hookReturnsManagerFlushResult"`
	PoolWaitReleasedAfterFlush       bool   `json:"poolWaitReleasedAfterFlush"`
}

type crawlerCompositionPersistence struct {
	TorrentTransactionStages        []string `json:"torrentTransactionStages"`
	TorrentChunkSizes               []uint64 `json:"torrentChunkSizes"`
	TorrentSingleTransaction        bool     `json:"torrentSingleTransaction"`
	TorrentScrapeFanoutAfterSuccess bool     `json:"torrentScrapeFanoutAfterSuccess"`
	TorrentScrapeFanoutAfterError   bool     `json:"torrentScrapeFanoutAfterError"`
	TorrentScrapeFanoutOrder        string   `json:"torrentScrapeFanoutOrder"`
	SourceChunkSize                 uint64   `json:"sourceChunkSize"`
	SourceWholeBatchTransaction     bool     `json:"sourceWholeBatchTransaction"`
	SourcePriorChunkCommitPossible  bool     `json:"sourcePriorChunkCommitPossible"`
	SourceRetry                     bool     `json:"sourceRetry"`
	PersistedMetricAfterSuccessOnly bool     `json:"persistedMetricAfterSuccessOnly"`
}

type crawlerCompositionASTSpec struct {
	key      string
	path     string
	kind     string
	name     string
	receiver string
}

func TestGenerateDHTCrawlerCompositionParity(t *testing.T) {
	fixture := crawlerCompositionSourceFixture(t)
	if fixture.ID != crawlerCompositionFixtureID || fixture.Subsystem != "dht_crawler_composition" ||
		fixture.Classification != "SOURCE_ONLY" || fixture.Execution != "SOURCE_INSPECTION" {
		t.Fatalf("unexpected fixture envelope: %+v", fixture)
	}
	crawlerCompositionReconcile(t, []crawlerCompositionFixture{fixture})
}

func crawlerCompositionSourceFixture(t *testing.T) crawlerCompositionFixture {
	t.Helper()
	if strconv.IntSize != 64 {
		t.Fatalf("composition scaling vectors require 64-bit int, got %d", strconv.IntSize)
	}
	return crawlerCompositionFixture{
		ID:             crawlerCompositionFixtureID,
		Subsystem:      "dht_crawler_composition",
		Classification: "SOURCE_ONLY",
		Execution:      "SOURCE_INSPECTION",
		Oracle: crawlerCompositionOracle{
			Composition:             "production_cross_stage_construction_and_lifecycle_source_contract",
			Determinism:             "typed_semantic_inventory_normalized_AST_and_exact_source_and_prerequisite_SHA256",
			ActualFunctionsExecuted: []string{},
			SourcePinnedFunctions: []string{
				"config.NewDefaultConfig", "factory.New", "crawler.start",
				"discovered.NewDiscoveredNodes", "batching.NewBatchingChannel",
				"buffered.NewBufferedConcurrentChannel", "buffered.Run",
				"metainforequester.New", "requestLimiter.Request", "protocol.RandomPeerID",
				"dhtfx.New", "protocol.RandomNodeIDWithClientSuffix",
				"blocking.New", "persist.runPersistTorrents", "persist.runPersistSources",
				"persist.persistScrapedTorrentSources",
			},
			ProductionFunctionsExecuted: false,
			NetworkExecuted:             false,
			DatabaseExecuted:            false,
			GoroutinesStarted:           false,
		},
		Input: crawlerCompositionInput{Kind: "source_contract"},
		Expected: crawlerCompositionExpected{
			Config: crawlerCompositionConfig{
				ScalingFactor: 10, DefaultBootstrapReseedSeconds: 60,
				FactoryBootstrapReseedSeconds: 600, FactoryUsesConfiguredBootstrapReseed: false,
				OldestNodeScanSeconds: 10, OldPeerThresholdSeconds: 900,
				SaveFilesThreshold: 100, SavePieces: false,
				RescrapeThresholdSeconds: 30 * 24 * 60 * 60, SoughtNodeRotationSeconds: 10,
			},
			Scaling: crawlerCompositionScaling{
				Arithmetic: "64_bit_source_expression_evaluation_without_channel_allocation",
				Formulas: []crawlerCompositionScalingFormula{
					{Name: "discovered_nodes", CapacityExpression: "int(100*ScalingFactor)"},
					{Name: "nodes_for_ping", CapacityExpression: "int(ScalingFactor)", ConcurrencyExpression: "int(ScalingFactor)"},
					{Name: "nodes_for_find_node", CapacityExpression: "10*int(ScalingFactor)", ConcurrencyExpression: "10*int(ScalingFactor)"},
					{Name: "nodes_for_sample_infohashes", CapacityExpression: "10*int(ScalingFactor)", ConcurrencyExpression: "10*int(ScalingFactor)"},
					{Name: "info_hash_triage", CapacityExpression: "10*int(ScalingFactor)"},
					{Name: "get_peers", CapacityExpression: "10*int(ScalingFactor)", ConcurrencyExpression: "20*int(ScalingFactor)"},
					{Name: "scrape", CapacityExpression: "10*int(ScalingFactor)", ConcurrencyExpression: "20*int(ScalingFactor)"},
					{Name: "request_meta_info", CapacityExpression: "10*int(ScalingFactor)", ConcurrencyExpression: "40*int(ScalingFactor)"},
				},
				Vectors: crawlerCompositionScalingVectors(),
			},
			Routes: []crawlerCompositionRoute{
				{Name: "discovered_nodes", Capacity: 1000, BatchSize: 10, BatchIntervalMilliseconds: 10, Implementation: "BatchingChannel"},
				{Name: "nodes_for_ping", Capacity: 10, Concurrency: 10, Implementation: "BufferedConcurrentChannel"},
				{Name: "nodes_for_find_node", Capacity: 100, Concurrency: 100, Implementation: "BufferedConcurrentChannel"},
				{Name: "nodes_for_sample_infohashes", Capacity: 100, Concurrency: 100, Implementation: "BufferedConcurrentChannel"},
				{Name: "info_hash_triage", Capacity: 100, BatchSize: 1000, BatchIntervalMilliseconds: 20_000, Implementation: "BatchingChannel"},
				{Name: "get_peers", Capacity: 100, Concurrency: 200, Implementation: "BufferedConcurrentChannel"},
				{Name: "scrape", Capacity: 100, Concurrency: 200, Implementation: "BufferedConcurrentChannel"},
				{Name: "request_meta_info", Capacity: 100, Concurrency: 400, Implementation: "BufferedConcurrentChannel"},
				{Name: "persist_torrents", Capacity: 1000, BatchSize: 1000, BatchIntervalMilliseconds: 60_000, Implementation: "BatchingChannel"},
				{Name: "persist_sources", Capacity: 1000, BatchSize: 1000, BatchIntervalMilliseconds: 60_000, Implementation: "BatchingChannel"},
			},
			CrawlerLaunches: crawlerCompositionLaunches(),
			Lifecycle: crawlerCompositionLifecycle{
				StartUsesBackgroundContext: true, CancelDeferredUntilStartReturns: true,
				StartWaitsOnlyForStopped: true, OnStopSetsActiveFalse: true, OnStopClosesStopped: true,
				OnStopJoinsCrawlerChildren: false, OnStopClosesPipelineInputs: false,
				BatchersStartDetachedAtConstruction: true, BatcherOutputCapacity: 1,
				ConcurrentCallbacksStartDetached: true, ConcurrentCallbacksJoined: false,
				LaunchOrderEvidence: "source_lexical_go_statement_order_only_not_runtime_goroutine_scheduling_order",
			},
			Requester: crawlerCompositionRequester{
				PeerIDGeneratedOncePerFactory: true, PeerIDGenerator: "protocol.RandomPeerID",
				PeerIDClientPrefix: "-BM0001-", PeerIDSeparateFromDHTNodeID: true,
				RequestTimeoutSeconds: 6, ConnectTimeoutSeconds: 3,
				WrapperCallOrder: []string{"requestLimiter", "requestLogger", "prometheusCollector", "requester"},
				LimiterKey:       "remote_IP_without_port", LimiterTokenIntervalMilliseconds: 500,
				LimiterBurst: 4, LimiterKeyCapacity: 1000, LimiterTTLSeconds: 20,
				ConfiguredKeyMutexSizeUsedByFactory: false,
				LoggerSampleTickSeconds:             60, LoggerSampleInitial: 10, LoggerSampleThereafter: 0,
				LimiterFailuresReachLoggerOrMetrics: false,
			},
			Blocking: crawlerCompositionBlocking{
				SharedByTriageAndRequestMetaInfo: true, TriageOperation: "Filter",
				BannedOperation: "Block", BannedFlushArgument: false, CrawlerOnStopCallsFlush: false,
				FactoryProvidesFlushHook: true, HookCallsIfInitialized: true,
				HookReturnsManagerFlushResult: true, PoolWaitReleasedAfterFlush: true,
			},
			Persistence: crawlerCompositionPersistence{
				TorrentTransactionStages: []string{"torrents", "torrent_files", "torrent_file_summary", "torrents_torrent_sources", "torrent_pieces_if_enabled", "queue_jobs"},
				TorrentChunkSizes:        []uint64{100, 100, 100, 100, 10, 10},
				TorrentSingleTransaction: true, TorrentScrapeFanoutAfterSuccess: true,
				TorrentScrapeFanoutAfterError: false, TorrentScrapeFanoutOrder: "Go_map_iteration_unspecified",
				SourceChunkSize: 100, SourceWholeBatchTransaction: false,
				SourcePriorChunkCommitPossible: true, SourceRetry: false,
				PersistedMetricAfterSuccessOnly: true,
			},
			NormalizedASTSHA256:       crawlerCompositionNormalizedASTDigests(t),
			SourceSHA256:              crawlerCompositionSourceDigests(t),
			PrerequisiteFixtureSHA256: crawlerCompositionPrerequisiteDigests(t),
			Nonclaims: []string{
				"no_production_function_channel_goroutine_Fx_hook_DNS_network_database_limiter_clock_logger_or_metric_execution",
				"no_runtime_goroutine_start_completion_or_map_iteration_order",
				"no_Go_shutdown_join_queue_drain_final_batch_flush_or_completed_side_effect_guarantee",
				"no_relative_order_claim_between_worker_registry_stop_and_Fx_app_hooks",
				"no_database_transaction_rows_affected_rollback_commit_durability_or_partial_chunk_runtime_evidence",
				"no_Rust_supervisor_application_deployment_or_production_readiness_claim",
			},
		},
	}
}

func crawlerCompositionScalingVectors() []crawlerCompositionScalingVector {
	maxNonnegativeDiscovery := uint(math.MaxInt64 / 100)
	return []crawlerCompositionScalingVector{
		crawlerCompositionScalingVectorFor(0),
		crawlerCompositionScalingVectorFor(2),
		crawlerCompositionScalingVectorFor(10),
		crawlerCompositionScalingVectorFor(maxNonnegativeDiscovery),
		crawlerCompositionScalingVectorFor(maxNonnegativeDiscovery + 1),
		crawlerCompositionScalingVectorFor(^uint(0)),
	}
}

func crawlerCompositionScalingVectorFor(scalingFactor uint) crawlerCompositionScalingVector {
	scaling := int(scalingFactor)
	return crawlerCompositionScalingVector{
		ScalingFactor:               uint64(scalingFactor),
		DiscoveryCapacity:           int64(int(100 * scalingFactor)),
		PingCapacity:                int64(scaling),
		PingConcurrency:             int64(scaling),
		FindNodeCapacity:            int64(10 * scaling),
		FindNodeConcurrency:         int64(10 * scaling),
		SampleInfohashesCapacity:    int64(10 * scaling),
		SampleInfohashesConcurrency: int64(10 * scaling),
		InfoHashTriageCapacity:      int64(10 * scaling),
		GetPeersCapacity:            int64(10 * scaling),
		GetPeersConcurrency:         int64(20 * scaling),
		ScrapeCapacity:              int64(10 * scaling),
		ScrapeConcurrency:           int64(20 * scaling),
		RequestMetaInfoCapacity:     int64(10 * scaling),
		RequestMetaInfoConcurrency:  int64(40 * scaling),
	}
}

func crawlerCompositionLaunches() []crawlerCompositionLaunch {
	names := []string{
		"rotateSoughtNodeID", "runDiscoveredNodes", "runPing", "runFindNode",
		"getNodesForFindNode", "runSampleInfoHashes", "getNodesForSampleInfoHashes",
		"runInfoHashTriage", "runGetPeers", "runRequestMetaInfo", "runScrape",
		"reseedBootstrapNodes", "runPersistTorrents", "runPersistSources", "getOldNodes",
	}
	launches := make([]crawlerCompositionLaunch, 0, len(names))
	for index, name := range names {
		launches = append(launches, crawlerCompositionLaunch{
			SourceOrder: index, Function: name, Detached: true, Joined: false,
		})
	}
	return launches
}

func crawlerCompositionNormalizedASTDigests(t *testing.T) map[string]string {
	t.Helper()
	specs := []crawlerCompositionASTSpec{
		{key: "config.NewDefaultConfig", path: "internal/dhtcrawler/config.go", kind: "func", name: "NewDefaultConfig"},
		{key: "factory.New", path: "internal/dhtcrawler/factory.go", kind: "func", name: "New"},
		{key: "crawler.start", path: "internal/dhtcrawler/crawler.go", kind: "func", name: "start", receiver: "*crawler"},
		{key: "discovered.NewDiscoveredNodes", path: "internal/dhtcrawler/discovered_nodes.go", kind: "func", name: "NewDiscoveredNodes"},
		{key: "batching.NewBatchingChannel", path: "internal/concurrency/batching_channel.go", kind: "func", name: "NewBatchingChannel"},
		{key: "buffered.NewBufferedConcurrentChannel", path: "internal/concurrency/buffered_concurrent_channel.go", kind: "func", name: "NewBufferedConcurrentChannel"},
		{key: "buffered.Run", path: "internal/concurrency/buffered_concurrent_channel.go", kind: "func", name: "Run", receiver: "bufferedConcurrentChannel[T]"},
		{key: "metainforequester.New", path: "internal/protocol/metainfo/metainforequester/factory.go", kind: "func", name: "New"},
		{key: "requestLimiter.Request", path: "internal/protocol/metainfo/metainforequester/limiter.go", kind: "func", name: "Request", receiver: "requestLimiter"},
		{key: "protocol.RandomPeerID", path: "internal/protocol/id.go", kind: "func", name: "RandomPeerID"},
		{key: "dhtfx.New", path: "internal/protocol/dht/dhtfx/module.go", kind: "func", name: "New"},
		{key: "protocol.RandomNodeIDWithClientSuffix", path: "internal/protocol/id.go", kind: "func", name: "RandomNodeIDWithClientSuffix"},
		{key: "blocking.New", path: "internal/blocking/factory.go", kind: "func", name: "New"},
		{key: "persist.runPersistTorrents", path: "internal/dhtcrawler/persist.go", kind: "func", name: "runPersistTorrents", receiver: "*crawler"},
		{key: "persist.runPersistSources", path: "internal/dhtcrawler/persist.go", kind: "func", name: "runPersistSources", receiver: "*crawler"},
		{key: "persist.persistScrapedTorrentSources", path: "internal/dhtcrawler/persist.go", kind: "func", name: "persistScrapedTorrentSources"},
	}
	digests := make(map[string]string, len(specs))
	for _, spec := range specs {
		node, files := crawlerCompositionFindASTNode(t, spec)
		var normalized bytes.Buffer
		if err := format.Node(&normalized, files, node); err != nil {
			t.Fatal(err)
		}
		digests[spec.key] = fmt.Sprintf("%x", sha256.Sum256(normalized.Bytes()))
	}
	return digests
}

func crawlerCompositionFindASTNode(t *testing.T, spec crawlerCompositionASTSpec) (ast.Node, *token.FileSet) {
	t.Helper()
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, filepath.Join(crawlerCompositionRoot(t), spec.path), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	matches := make([]ast.Node, 0, 1)
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || spec.kind != "func" || function.Name.Name != spec.name {
			continue
		}
		if crawlerCompositionReceiverShape(t, files, function) != spec.receiver {
			continue
		}
		matches = append(matches, function)
	}
	if len(matches) != 1 {
		t.Fatalf("AST %s matches = %d, want exactly one", spec.key, len(matches))
	}
	return matches[0], files
}

func crawlerCompositionReceiverShape(t *testing.T, files *token.FileSet, function *ast.FuncDecl) string {
	t.Helper()
	if function.Recv == nil || len(function.Recv.List) != 1 {
		return ""
	}
	var encoded bytes.Buffer
	if err := format.Node(&encoded, files, function.Recv.List[0].Type); err != nil {
		t.Fatal(err)
	}
	return encoded.String()
}

func crawlerCompositionSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	paths := []string{
		"internal/blocking/factory.go",
		"internal/concurrency/batching_channel.go",
		"internal/concurrency/buffered_concurrent_channel.go",
		"internal/dhtcrawler/config.go",
		"internal/dhtcrawler/crawler.go",
		"internal/dhtcrawler/discovered_nodes.go",
		"internal/dhtcrawler/factory.go",
		"internal/dhtcrawler/infohash_triage.go",
		"internal/dhtcrawler/persist.go",
		"internal/dhtcrawler/request_meta_info.go",
		"internal/protocol/id.go",
		"internal/protocol/dht/dhtfx/module.go",
		"internal/protocol/metainfo/metainforequester/config.go",
		"internal/protocol/metainfo/metainforequester/factory.go",
		"internal/protocol/metainfo/metainforequester/limiter.go",
		"internal/protocol/metainfo/metainforequester/logger.go",
		"internal/protocol/metainfo/metainforequester/prometheus_collector.go",
	}
	digests := make(map[string]string, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(filepath.Join(crawlerCompositionRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		digests[path] = fmt.Sprintf("%x", sha256.Sum256(contents))
	}
	return digests
}

func crawlerCompositionPrerequisiteDigests(t *testing.T) map[string]string {
	t.Helper()
	want := map[string]string{
		"testdata/parity/dht/dht_crawler_info_hash_triage.jsonl":  "52eda840f872225cc34f8cf12edc2e4621e8a1fef569abf34a50f4a3bd9896f8",
		"testdata/parity/dht/dht_crawler_get_peers.jsonl":         "82b694fece9e46c05aefaab76bc05b78462bc04824bf6b83bb77eb544b7f0844",
		"testdata/parity/dht/dht_crawler_scrape.jsonl":            "d434306fd60678be95cabd53d59ea152f6a013bf2e486f4bb2456aa8da2c6d9b",
		"testdata/parity/dht/dht_crawler_request_meta_info.jsonl": "03ce2ab0da2b0f9ba1173b8ba52481a903265ca6862f957b40490cf67a9e4ec5",
		"testdata/parity/dht/dht_crawler_persist_sources.jsonl":   "01acacdc5ccc425bda88e87643328101499af3873f3a52c7eef2f46a92697bd9",
		"testdata/parity/dht/dht_crawler_persist_torrents.jsonl":  "40adced4a96a860354d8ba74c412566e2a72979261bd674994c4ef18d6680bc5",
		"testdata/parity/dht/metainfo_requester.jsonl":            "990f4d503065ed08689df37881817386874f12cda2fdaeaeb56c05e12bbcc80e",
		"testdata/parity/dht/keyed_limiter.jsonl":                 "53787bb82f1b4c51519a4e412848ead5d9e03a316bc8403a928004f2446bfac8",
		"testdata/parity/dht/dht_info_hash_block_filter.jsonl":    "cc17edc11e5a21fe668d1067d2cf7413643bfdc8b81b0d5e97e5830afb1a51b4",
	}
	for path, expected := range want {
		contents, err := os.ReadFile(filepath.Join(crawlerCompositionRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		if actual := fmt.Sprintf("%x", sha256.Sum256(contents)); actual != expected {
			t.Fatalf("%s SHA-256 = %s, want %s", path, actual, expected)
		}
	}
	return want
}

func crawlerCompositionRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve composition generator source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
}

func crawlerCompositionReconcile(t *testing.T, fixtures []crawlerCompositionFixture) {
	t.Helper()
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	for _, fixture := range fixtures {
		if err := encoder.Encode(fixture); err != nil {
			t.Fatal(err)
		}
	}
	if encoded.Len() == 0 || encoded.Bytes()[encoded.Len()-1] != '\n' || bytes.Contains(encoded.Bytes(), []byte("\r\n")) {
		t.Fatal("composition fixture must be nonempty LF-only JSONL with a final LF")
	}
	crawlerCompositionValidateStrictJSONL(t, encoded.Bytes(), fixtures)
	actualHash := fmt.Sprintf("%x", sha256.Sum256(encoded.Bytes()))
	if !*updateDHTCrawlerCompositionParity && crawlerCompositionFixtureSHA256 != "" &&
		actualHash != crawlerCompositionFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerCompositionFixtureSHA256)
	}
	path := filepath.Join(crawlerCompositionRoot(t), "testdata/parity/dht/dht_crawler_composition.jsonl")
	if *updateDHTCrawlerCompositionParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-composition-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler composition fixture is stale; rerun with -update-dht-crawler-composition-parity")
	}
}

func crawlerCompositionValidateStrictJSONL(t *testing.T, contents []byte, want []crawlerCompositionFixture) {
	t.Helper()
	scanner := bufio.NewScanner(bytes.NewReader(contents))
	row := 0
	for scanner.Scan() {
		if row >= len(want) {
			t.Fatalf("strict decoded row count exceeds %d", len(want))
		}
		if err := crawlerCompositionValidateEncoded(scanner.Bytes(), want[row]); err != nil {
			t.Fatalf("strict decode row %d: %v", row+1, err)
		}
		row++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if row != len(want) {
		t.Fatalf("strict decoded row count = %d, want %d", row, len(want))
	}
}

func crawlerCompositionValidateEncoded(contents []byte, want crawlerCompositionFixture) error {
	actualRaw, err := crawlerCompositionDecodeJSONNoDuplicates(contents)
	if err != nil {
		return err
	}
	expectedBytes, err := json.Marshal(want)
	if err != nil {
		return err
	}
	expectedRaw, err := crawlerCompositionDecodeJSONNoDuplicates(expectedBytes)
	if err != nil {
		return fmt.Errorf("decode generated expectation: %w", err)
	}
	if !reflect.DeepEqual(actualRaw, expectedRaw) {
		return fmt.Errorf("object keys, required values, array membership, or digest-map keys differ from the generated contract")
	}

	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var fixture crawlerCompositionFixture
	if err := decoder.Decode(&fixture); err != nil {
		return err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing JSON: %w", err)
	}
	for name, digests := range map[string]map[string]string{
		"normalizedAstSha256":       fixture.Expected.NormalizedASTSHA256,
		"sourceSha256":              fixture.Expected.SourceSHA256,
		"prerequisiteFixtureSha256": fixture.Expected.PrerequisiteFixtureSHA256,
	} {
		if err := crawlerCompositionValidateDigests(name, digests); err != nil {
			return err
		}
	}
	return nil
}

func crawlerCompositionDecodeJSONNoDuplicates(contents []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.UseNumber()
	value, err := crawlerCompositionDecodeJSONValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON: %w", err)
	}
	return value, nil
}

func crawlerCompositionDecodeJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, fmt.Errorf("object key has type %T", keyToken)
			}
			if _, duplicate := object[key]; duplicate {
				return nil, fmt.Errorf("duplicate object key %q", key)
			}
			value, err := crawlerCompositionDecodeJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		end, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if end != json.Delim('}') {
			return nil, fmt.Errorf("object ended with %v", end)
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, err := crawlerCompositionDecodeJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		end, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if end != json.Delim(']') {
			return nil, fmt.Errorf("array ended with %v", end)
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected delimiter %q", delimiter)
	}
}

func crawlerCompositionValidateDigests(name string, digests map[string]string) error {
	if len(digests) == 0 {
		return fmt.Errorf("%s must not be empty", name)
	}
	for key, digest := range digests {
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size || digest != strings.ToLower(digest) {
			return fmt.Errorf("%s[%q] is not a lowercase SHA-256", name, key)
		}
	}
	return nil
}

func TestDHTCrawlerCompositionSchemaIsExact(t *testing.T) {
	fixture := crawlerCompositionSourceFixture(t)
	encoded, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "unknown top-level key", mutate: func(value map[string]any) { value["unknown"] = true }},
		{name: "unknown nested key", mutate: func(value map[string]any) {
			value["expected"].(map[string]any)["routes"].([]any)[0].(map[string]any)["unknown"] = true
		}},
		{name: "unknown scaling key", mutate: func(value map[string]any) {
			value["expected"].(map[string]any)["scaling"].(map[string]any)["unknown"] = true
		}},
		{name: "missing required scalar", mutate: func(value map[string]any) { delete(value, "id") }},
		{name: "missing nested scalar", mutate: func(value map[string]any) {
			delete(value["expected"].(map[string]any)["config"].(map[string]any), "scalingFactor")
		}},
		{name: "missing scaling vectors", mutate: func(value map[string]any) {
			delete(value["expected"].(map[string]any)["scaling"].(map[string]any), "vectors")
		}},
		{name: "null object", mutate: func(value map[string]any) { value["oracle"] = nil }},
		{name: "null scalar", mutate: func(value map[string]any) { value["subsystem"] = nil }},
		{name: "missing digest key", mutate: func(value map[string]any) {
			delete(value["expected"].(map[string]any)["sourceSha256"].(map[string]any), "internal/blocking/factory.go")
		}},
		{name: "foreign digest key", mutate: func(value map[string]any) {
			value["expected"].(map[string]any)["sourceSha256"].(map[string]any)["foreign.go"] = strings.Repeat("0", 64)
		}},
		{name: "invalid digest", mutate: func(value map[string]any) {
			value["expected"].(map[string]any)["normalizedAstSha256"].(map[string]any)["factory.New"] = "ABC"
		}},
		{name: "short routes", mutate: func(value map[string]any) {
			expected := value["expected"].(map[string]any)
			expected["routes"] = expected["routes"].([]any)[1:]
		}},
		{name: "duplicate launch", mutate: func(value map[string]any) {
			expected := value["expected"].(map[string]any)
			launches := expected["crawlerLaunches"].([]any)
			expected["crawlerLaunches"] = append(launches, launches[0])
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			var candidate map[string]any
			if err := json.Unmarshal(encoded, &candidate); err != nil {
				t.Fatal(err)
			}
			mutation.mutate(candidate)
			mutated, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if err := crawlerCompositionValidateEncoded(mutated, fixture); err == nil {
				t.Fatal("mutated fixture unexpectedly validated")
			}
		})
	}

	rawMutations := map[string][]byte{
		"duplicate top-level key": append([]byte(`{"id":"duplicate",`), encoded[1:]...),
		"duplicate nested key": bytes.Replace(
			encoded,
			[]byte(`"config":{`),
			[]byte(`"config":{},"config":{`),
			1,
		),
		"trailing JSON": append(append([]byte(nil), encoded...), []byte(` {}`)...),
	}
	for name, mutated := range rawMutations {
		t.Run(name, func(t *testing.T) {
			if err := crawlerCompositionValidateEncoded(mutated, fixture); err == nil {
				t.Fatal("raw-mutated fixture unexpectedly validated")
			}
		})
	}
}
