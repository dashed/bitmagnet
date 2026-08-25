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
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/bloom"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	pdht "github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
)

var updateDHTCrawlerPersistSourcesParity = flag.Bool(
	"update-dht-crawler-persist-sources-parity",
	false,
	"rewrite the DHT crawler persist-sources parity fixture",
)

const crawlerPersistSourcesFixtureSHA256 = "01acacdc5ccc425bda88e87643328101499af3873f3a52c7eef2f46a92697bd9"

var crawlerPersistSourcesFixtureIDs = [...]string{
	"production_source_factory_batcher_lifecycle_model_sql_and_schema_contract",
	"empty_and_directional_filters_project_valid_counts_null_optionals_and_bloom_direction",
	"one_bit_hash_collision_rounds_half_up_to_one_while_truncation_would_be_zero",
	"ordered_duplicate_batch_first_occurrence_wins_in_first_unique_order",
}

var crawlerPersistSourcesExpectedNormalizedASTSHA256 = map[string]string{
	"batching.In":                          "f5ef939724dc08bc0fa39e9fa2e0863e45acd1c965609ad91fa7082fd6632b21",
	"batching.NewBatchingChannel":          "2c9a3fa894f82680a8cb8437d8dbad6d3bc2da9a7594c83553ef7650dd472dc6",
	"batching.Out":                         "f677733fd65c621331747365d30bc29503cda90a21e5aba68ece706afd5d2e3c",
	"batching.batch":                       "ebedd32544fc4a53c3cb016fd883da2e76267dd492a7c5f88ba2ebcf8232858c",
	"batching.flush":                       "3c72fb1d8c6d52bfed5b60a796d5bfee0e13da3b745c220ac01467a88de1f274",
	"bloom.FromScrape":                     "7298c86e1af2c667f8ae43775229426e70574a33dd4148ea2a71888bfe66f20b",
	"crawler.infoHashWithScrape":           "c9f4fdef915a61322eeaab348afd5896744000a5382416f474de44f21a6f835c",
	"crawler.nodeHasPeersForHash":          "1e2206b038dd5c1b70dff5a29cdf044ad7133b4876db75723081ab37c3d3da58",
	"crawler.start":                        "d61a318ce626352ee4f5cd5dd48191d767bbfe45b6a9def673cd185eada4f67b",
	"factory.New":                          "0204a00fd63b275339d63d622865858571c153bc81fc738784a78e1c150fec80",
	"model.NewNullUint":                    "bba8e9dedb19e3e33c3dfed0ad327aa9e11892eaa6a08f3495b31062dfcff33f",
	"model.TorrentsTorrentSource":          "f71036cb64dfaa18994e0caa7fe63e394a93e3f29cf00312ce7f7d2e2cf358e5",
	"persist.createTorrentSourceModel":     "288ba786fbb6da0578c1164de0bba17bc5376e387996e3b02c54bdb2774f79f7",
	"persist.persistScrapedTorrentSources": "e3b5338f2bd11789760caa263f1880535165ce58649bce0ef364941c04454097",
	"persist.runPersistSources":            "07ad92a09673d00523cc463c4c6b3cf6f31881c3ed279e0d77e3ce2c0659dc6a",
	"protocol.ID.String":                   "c8e7761bfacaedb901406cffb17a1816adbc162e174f19a5678e20817f339126",
}

type crawlerPersistSourcesFixture struct {
	ID             string                        `json:"id"`
	Subsystem      string                        `json:"subsystem"`
	Classification string                        `json:"classification"`
	Oracle         crawlerPersistSourcesOracle   `json:"oracle"`
	Input          crawlerPersistSourcesInput    `json:"input"`
	Expected       crawlerPersistSourcesExpected `json:"expected"`
}

type crawlerPersistSourcesOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Harness     string `json:"harness"`
	Database    string `json:"database"`
	Clock       string `json:"clock"`
}

type crawlerPersistSourcesInput struct {
	Kind    string                        `json:"kind"`
	Scrapes []crawlerPersistSourcesScrape `json:"scrapes"`
}

type crawlerPersistSourcesScrape struct {
	InfoHash string                           `json:"infoHash"`
	Node     crawlerPersistSourcesAddress     `json:"node"`
	Seeders  crawlerPersistSourcesFilterInput `json:"seeders"`
	Peers    crawlerPersistSourcesFilterInput `json:"peers"`
}

type crawlerPersistSourcesAddress struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope"`
}

type crawlerPersistSourcesFilterInput struct {
	RawIPs []string                           `json:"rawIps"`
	Ranges []crawlerPersistSourcesFilterRange `json:"ranges"`
}

type crawlerPersistSourcesFilterRange struct {
	Base  string `json:"base"`
	Count int    `json:"count"`
}

type crawlerPersistSourcesExpected struct {
	Models                    []crawlerPersistSourcesModel `json:"models"`
	BloomObservations         []crawlerPersistSourcesBloom `json:"bloomObservations"`
	ConverterCallInfoHashes   []string                     `json:"converterCallInfoHashes"`
	DuplicateInfoHashes       []string                     `json:"duplicateInfoHashes"`
	ConversionErrors          []string                     `json:"conversionErrors"`
	FirstOccurrenceOrder      []string                     `json:"firstOccurrenceOrder"`
	RunPersistSourcesExecuted bool                         `json:"runPersistSourcesExecuted"`
	Source                    *crawlerPersistSourcesSource `json:"source,omitempty"`
}

type crawlerPersistSourcesModel struct {
	Source                  string `json:"source"`
	InfoHash                string `json:"infoHash"`
	Seeders                 uint   `json:"seeders"`
	SeedersValid            bool   `json:"seedersValid"`
	Leechers                uint   `json:"leechers"`
	LeechersValid           bool   `json:"leechersValid"`
	SeenCount               uint   `json:"seenCount"`
	ImportIDValid           bool   `json:"importIdValid"`
	PublishedAtValid        bool   `json:"publishedAtValid"`
	CreatedAtZero           bool   `json:"createdAtZero"`
	UpdatedAtZero           bool   `json:"updatedAtZero"`
	SourceNodeRetained      bool   `json:"sourceNodeRetained"`
	RawSeedersBloomRetained bool   `json:"rawSeedersBloomRetained"`
	RawPeersBloomRetained   bool   `json:"rawPeersBloomRetained"`
}

type crawlerPersistSourcesBloom struct {
	InfoHash            string `json:"infoHash"`
	SeedersBloomSHA256  string `json:"seedersBloomSha256"`
	PeersBloomSHA256    string `json:"peersBloomSha256"`
	SeedersApproximated uint32 `json:"seedersApproximated"`
	PeersApproximated   uint32 `json:"peersApproximated"`
}

type crawlerPersistSourcesSource struct {
	Factory             crawlerPersistSourcesFactoryContract    `json:"factory"`
	Batcher             crawlerPersistSourcesBatcherContract    `json:"batcher"`
	Lifecycle           crawlerPersistSourcesLifecycleContract  `json:"lifecycle"`
	Worker              crawlerPersistSourcesWorkerContract     `json:"worker"`
	Model               crawlerPersistSourcesModelContract      `json:"model"`
	Repository          crawlerPersistSourcesRepositoryContract `json:"repository"`
	Schema              crawlerPersistSourcesSchemaContract     `json:"schema"`
	Dependencies        crawlerPersistSourcesDependencies       `json:"dependencies"`
	NormalizedASTSHA256 map[string]string                       `json:"normalizedAstSha256"`
	SourceSHA256        map[string]string                       `json:"sourceSha256"`
	PrerequisiteSHA256  map[string]string                       `json:"prerequisiteSha256"`
	Nonclaims           []string                                `json:"nonclaims"`
	Evidence            string                                  `json:"evidence"`
}

type crawlerPersistSourcesFactoryContract struct {
	InputCapacity       int      `json:"inputCapacity"`
	MaximumBatchSize    int      `json:"maximumBatchSize"`
	BatchIntervalMillis int      `json:"batchIntervalMillis"`
	OutputCapacity      int      `json:"outputCapacity"`
	ConfigurationFields []string `json:"configurationFields"`
	Hardcoded           bool     `json:"hardcoded"`
}

type crawlerPersistSourcesBatcherContract struct {
	FlushAtMaximumSize               bool   `json:"flushAtMaximumSize"`
	FlushOnNonemptyTicker            bool   `json:"flushOnNonemptyTicker"`
	TickerStartsAtConstruction       bool   `json:"tickerStartsAtConstruction"`
	TickerResetsAfterFlush           bool   `json:"tickerResetsAfterFlush"`
	FlushBlocksOnOutput              bool   `json:"flushBlocksOnOutput"`
	ContextAware                     bool   `json:"contextAware"`
	InputCloseExitsLoop              bool   `json:"inputCloseExitsLoop"`
	ClosedInputSourceOutcome         string `json:"closedInputSourceOutcome"`
	OutputReceiveChecksOpenBoolean   bool   `json:"outputReceiveChecksOpenBoolean"`
	RawInputCapacityIsTotalRetention bool   `json:"rawInputCapacityIsTotalRetention"`
}

type crawlerPersistSourcesLifecycleContract struct {
	StartLaunchesCrawlerDetached       bool `json:"startLaunchesCrawlerDetached"`
	CrawlerLaunchesWorkerDetached      bool `json:"crawlerLaunchesWorkerDetached"`
	StartWaitsOnlyForStopped           bool `json:"startWaitsOnlyForStopped"`
	SharedContextCancelledAfterStopped bool `json:"sharedContextCancelledAfterStopped"`
	StopClosesBatcherInput             bool `json:"stopClosesBatcherInput"`
	StopDrainsPersistSources           bool `json:"stopDrainsPersistSources"`
	StopJoinsWorkerOrBatcher           bool `json:"stopJoinsWorkerOrBatcher"`
}

type crawlerPersistSourcesWorkerContract struct {
	FirstOccurrenceWins                bool   `json:"firstOccurrenceWins"`
	FirstUniqueOrderPreserved          bool   `json:"firstUniqueOrderPreserved"`
	DuplicateKey                       string `json:"duplicateKey"`
	ConversionErrorLoggedAndSkipped    bool   `json:"conversionErrorLoggedAndSkipped"`
	CurrentConversionCanReturnError    bool   `json:"currentConversionCanReturnError"`
	RepositoryCalledForEmptyModels     bool   `json:"repositoryCalledForEmptyModels"`
	RepositoryErrorLogged              bool   `json:"repositoryErrorLogged"`
	RepositoryErrorStopsWorker         bool   `json:"repositoryErrorStopsWorker"`
	RepositoryErrorRetriedOrRequeued   bool   `json:"repositoryErrorRetriedOrRequeued"`
	MetricEntity                       string `json:"metricEntity"`
	MetricCountsPreparedUniqueModels   bool   `json:"metricCountsPreparedUniqueModels"`
	MetricCountsActualAffectedRows     bool   `json:"metricCountsActualAffectedRows"`
	MetricIncrementedOnRepositoryError bool   `json:"metricIncrementedOnRepositoryError"`
}

type crawlerPersistSourcesModelContract struct {
	Source                    string `json:"source"`
	SeedersFrom               string `json:"seedersFrom"`
	LeechersFrom              string `json:"leechersFrom"`
	SubtractsSeedersFromPeers bool   `json:"subtractsSeedersFromPeers"`
	CountsAlwaysValid         bool   `json:"countsAlwaysValid"`
	SeenCount                 uint   `json:"seenCount"`
	InfoHashEncodingForSQL    string `json:"infoHashEncodingForSql"`
	SourceNodeRetained        bool   `json:"sourceNodeRetained"`
	RawBloomsRetained         bool   `json:"rawBloomsRetained"`
	ImportIDSet               bool   `json:"importIdSet"`
	PublishedAtSet            bool   `json:"publishedAtSet"`
	ModelCreatedUpdatedAtSet  bool   `json:"modelCreatedUpdatedAtSet"`
}

type crawlerPersistSourcesRepositoryContract struct {
	ChunkSize                       int      `json:"chunkSize"`
	ArgumentsPerRow                 int      `json:"argumentsPerRow"`
	OneTimestampPerInvocation       bool     `json:"oneTimestampPerInvocation"`
	ExplicitTransaction             bool     `json:"explicitTransaction"`
	MissingParentOutcome            string   `json:"missingParentOutcome"`
	ConflictTarget                  []string `json:"conflictTarget"`
	ConflictUpdatedColumns          []string `json:"conflictUpdatedColumns"`
	ConflictPreservedColumns        []string `json:"conflictPreservedColumns"`
	ConflictSeenCountExpression     string   `json:"conflictSeenCountExpression"`
	FirstExecErrorStopsChunks       bool     `json:"firstExecErrorStopsChunks"`
	EarlierChunksCanRemainCommitted bool     `json:"earlierChunksCanRemainCommitted"`
	RetryOrRequeue                  bool     `json:"retryOrRequeue"`
	OneRowSQL                       string   `json:"oneRowSql"`
}

type crawlerPersistSourcesSchemaContract struct {
	Table                  string                              `json:"table"`
	Columns                []crawlerPersistSourcesSchemaColumn `json:"columns"`
	PrimaryKey             []string                            `json:"primaryKey"`
	RawBloomColumnsPresent bool                                `json:"rawBloomColumnsPresent"`
}

type crawlerPersistSourcesSchemaColumn struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Nullable  bool   `json:"nullable"`
	Default   string `json:"default"`
	Reference string `json:"reference"`
}

type crawlerPersistSourcesDependencies struct {
	GoModBloomLine      string `json:"goModBloomLine"`
	GoSumBloomLine      string `json:"goSumBloomLine"`
	GoSumBloomGoModLine string `json:"goSumBloomGoModLine"`
	BloomApproximation  string `json:"bloomApproximation"`
}

type crawlerPersistSourcesASTSpec struct {
	key  string
	path string
	kind string
	name string
}

func TestGenerateDHTCrawlerPersistSourcesParity(t *testing.T) {
	fixtures := []crawlerPersistSourcesFixture{
		crawlerPersistSourcesSourceFixture(t),
		crawlerPersistSourcesRuntimeFixture(t, crawlerPersistSourcesFixtureIDs[1], "model_conversion", []crawlerPersistSourcesScrape{
			crawlerPersistSourcesScrapeInput(1, "192.0.2.1:7001", crawlerPersistSourcesFilterInput{}, crawlerPersistSourcesFilterInput{}),
			crawlerPersistSourcesScrapeInput(2, "[fe80::2%42]:7002",
				crawlerPersistSourcesFilterInput{RawIPs: []string{"7f000001"}},
				crawlerPersistSourcesFilterInput{RawIPs: []string{"20010db8000000000000000000000001", "c0000209"}}),
		}),
		crawlerPersistSourcesRuntimeFixture(t, crawlerPersistSourcesFixtureIDs[2], "model_conversion", []crawlerPersistSourcesScrape{
			crawlerPersistSourcesScrapeInput(3, "198.51.100.3:7003",
				crawlerPersistSourcesFilterInput{RawIPs: []string{"0a0002ae"}}, crawlerPersistSourcesFilterInput{}),
		}),
		crawlerPersistSourcesRuntimeFixture(t, crawlerPersistSourcesFixtureIDs[3], "source_pinned_first_wins_loop_harness", []crawlerPersistSourcesScrape{
			crawlerPersistSourcesScrapeInput(11, "192.0.2.11:7011", crawlerPersistSourcesFilterInput{RawIPs: []string{"7f000001"}}, crawlerPersistSourcesFilterInput{}),
			crawlerPersistSourcesScrapeInput(12, "192.0.2.12:7012", crawlerPersistSourcesFilterInput{}, crawlerPersistSourcesFilterInput{RawIPs: []string{"c0000209"}}),
			crawlerPersistSourcesScrapeInput(11, "[fe80::11%77]:7111", crawlerPersistSourcesFilterInput{Ranges: []crawlerPersistSourcesFilterRange{{Base: "c0000200", Count: 256}, {Base: "20010db8000000000000000000000000", Count: 1000}}}, crawlerPersistSourcesFilterInput{RawIPs: []string{"7f000001", "c0000209"}}),
			crawlerPersistSourcesScrapeInput(13, "192.0.2.13:7013", crawlerPersistSourcesFilterInput{RawIPs: []string{"20010db8000000000000000000000001", "20010db8000000000000000000000002"}}, crawlerPersistSourcesFilterInput{RawIPs: []string{"7f000001"}}),
			crawlerPersistSourcesScrapeInput(12, "192.0.2.212:7212", crawlerPersistSourcesFilterInput{RawIPs: []string{"c0000209"}}, crawlerPersistSourcesFilterInput{Ranges: []crawlerPersistSourcesFilterRange{{Base: "c0000200", Count: 256}}}),
		}),
	}

	if len(fixtures) != len(crawlerPersistSourcesFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerPersistSourcesFixtureIDs))
	}
	wantClassifications := [...]string{"SOURCE_ONLY", "RUNTIME_EXACT", "RUNTIME_EXACT", "RUNTIME_EXACT_TEST_HARNESS"}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerPersistSourcesFixtureIDs[index] {
			t.Fatalf("fixture %d ID = %q, want %q", index, fixture.ID, crawlerPersistSourcesFixtureIDs[index])
		}
		if fixture.Subsystem != "dht_crawler_persist_sources" || fixture.Classification != wantClassifications[index] {
			t.Fatalf("fixture %s subsystem/classification = %q/%q", fixture.ID, fixture.Subsystem, fixture.Classification)
		}
	}

	crawlerPersistSourcesReconcile(t, fixtures)
}

func crawlerPersistSourcesSourceFixture(t *testing.T) crawlerPersistSourcesFixture {
	return crawlerPersistSourcesFixture{
		ID: crawlerPersistSourcesFixtureIDs[0], Subsystem: "dht_crawler_persist_sources", Classification: "SOURCE_ONLY",
		Oracle: crawlerPersistSourcesOracle{
			Composition: "exact_production_source_AST_dependency_schema_factory_batcher_lifecycle_model_and_SQL_freshness_gate",
			Determinism: "normalized_AST_plus_source_prerequisite_and_fixture_SHA256",
			Harness:     "source_only_no_worker_or_database_execution", Database: "source_contract_only_without_live_PostgreSQL", Clock: "source_contract_only_for_time_New_and_ticker",
		},
		Input: crawlerPersistSourcesInput{Kind: "source_contract", Scrapes: []crawlerPersistSourcesScrape{}},
		Expected: crawlerPersistSourcesExpected{
			Models: []crawlerPersistSourcesModel{}, BloomObservations: []crawlerPersistSourcesBloom{}, ConverterCallInfoHashes: []string{},
			DuplicateInfoHashes: []string{}, ConversionErrors: []string{}, FirstOccurrenceOrder: []string{}, RunPersistSourcesExecuted: false,
			Source: &crawlerPersistSourcesSource{
				Factory: crawlerPersistSourcesFactoryContract{
					InputCapacity: 1000, MaximumBatchSize: 1000, BatchIntervalMillis: 60000, OutputCapacity: 1,
					ConfigurationFields: []string{}, Hardcoded: true,
				},
				Batcher: crawlerPersistSourcesBatcherContract{
					FlushAtMaximumSize: true, FlushOnNonemptyTicker: true, TickerStartsAtConstruction: true,
					TickerResetsAfterFlush: true, FlushBlocksOnOutput: true, ContextAware: false,
					InputCloseExitsLoop: false, ClosedInputSourceOutcome: "unlabeled_break_exits_select_only_and_closed_input_spins_without_closing_output",
					OutputReceiveChecksOpenBoolean: false, RawInputCapacityIsTotalRetention: false,
				},
				Lifecycle: crawlerPersistSourcesLifecycleContract{
					StartLaunchesCrawlerDetached: true, CrawlerLaunchesWorkerDetached: true, StartWaitsOnlyForStopped: true,
					SharedContextCancelledAfterStopped: true, StopClosesBatcherInput: false, StopDrainsPersistSources: false,
					StopJoinsWorkerOrBatcher: false,
				},
				Worker: crawlerPersistSourcesWorkerContract{
					FirstOccurrenceWins: true, FirstUniqueOrderPreserved: true, DuplicateKey: "protocol.ID_info_hash",
					ConversionErrorLoggedAndSkipped: true, CurrentConversionCanReturnError: false, RepositoryCalledForEmptyModels: true,
					RepositoryErrorLogged: true, RepositoryErrorStopsWorker: false, RepositoryErrorRetriedOrRequeued: false,
					MetricEntity: "TorrentsTorrentSource", MetricCountsPreparedUniqueModels: true,
					MetricCountsActualAffectedRows: false, MetricIncrementedOnRepositoryError: false,
				},
				Model: crawlerPersistSourcesModelContract{
					Source: "dht", SeedersFrom: "bfsd.ApproximatedSize", LeechersFrom: "bfpe.ApproximatedSize",
					SubtractsSeedersFromPeers: false, CountsAlwaysValid: true, SeenCount: 1,
					InfoHashEncodingForSQL: "lowercase_40_hex_then_PostgreSQL_decode_hex", SourceNodeRetained: false,
					RawBloomsRetained: false, ImportIDSet: false, PublishedAtSet: false, ModelCreatedUpdatedAtSet: false,
				},
				Repository: crawlerPersistSourcesRepositoryContract{
					ChunkSize: 100, ArgumentsPerRow: 8, OneTimestampPerInvocation: true, ExplicitTransaction: false,
					MissingParentOutcome: "silently_skipped_by_WHERE_EXISTS", ConflictTarget: []string{"info_hash", "source"},
					ConflictUpdatedColumns:      []string{"seeders", "leechers", "published_at", "updated_at", "seen_count"},
					ConflictPreservedColumns:    []string{"created_at", "import_id"},
					ConflictSeenCountExpression: "torrents_torrent_sources.seen_count + 1", FirstExecErrorStopsChunks: true,
					EarlierChunksCanRemainCommitted: true, RetryOrRequeue: false, OneRowSQL: crawlerPersistSourcesOneRowSQL(),
				},
				Schema: crawlerPersistSourcesSchemaContract{
					Table: "torrents_torrent_sources",
					Columns: []crawlerPersistSourcesSchemaColumn{
						{Name: "source", Type: "text", Nullable: false, Default: "", Reference: "torrent_sources_on_delete_cascade"},
						{Name: "info_hash", Type: "bytea", Nullable: false, Default: "", Reference: "torrents_on_delete_cascade"},
						{Name: "import_id", Type: "text", Nullable: true, Default: "", Reference: ""},
						{Name: "seeders", Type: "integer", Nullable: true, Default: "", Reference: ""},
						{Name: "leechers", Type: "integer", Nullable: true, Default: "", Reference: ""},
						{Name: "published_at", Type: "timestamp_with_time_zone", Nullable: true, Default: "", Reference: ""},
						{Name: "created_at", Type: "timestamp_with_time_zone", Nullable: false, Default: "", Reference: ""},
						{Name: "updated_at", Type: "timestamp_with_time_zone", Nullable: false, Default: "", Reference: ""},
						{Name: "seen_count", Type: "integer", Nullable: false, Default: "1", Reference: ""},
					},
					PrimaryKey: []string{"source", "info_hash"}, RawBloomColumnsPresent: false,
				},
				Dependencies: crawlerPersistSourcesDependencies{
					GoModBloomLine: crawlerPersistSourcesDependencyLine(
						t, "go.mod", "github.com/bits-and-blooms/bloom/v3 v3.7.0",
					),
					GoSumBloomLine: crawlerPersistSourcesDependencyLine(
						t, "go.sum", "github.com/bits-and-blooms/bloom/v3 v3.7.0 h1:",
					),
					GoSumBloomGoModLine: crawlerPersistSourcesDependencyLine(
						t, "go.sum", "github.com/bits-and-blooms/bloom/v3 v3.7.0/go.mod h1:",
					),
					BloomApproximation: "uint32_floor_negative_m_over_k_log_one_minus_x_over_m_plus_half_for_finite_filters",
				},
				NormalizedASTSHA256: crawlerPersistSourcesNormalizedASTDigests(t),
				SourceSHA256:        crawlerPersistSourcesSourceDigests(t),
				PrerequisiteSHA256:  crawlerPersistSourcesPrerequisiteDigests(t),
				Nonclaims: []string{
					"actual_runPersistSources_runtime_execution",
					"live_PostgreSQL_SQL_execution_schema_plan_index_locking_or_affected_row_count",
					"database_transactionality_beyond_source_observation_of_no_explicit_transaction",
					"exact_wall_clock_time_New_or_batching_ticker_elapsed_schedule",
					"ready_select_tie_winner_goroutine_scheduling_or_channel_fairness",
					"shutdown_drain_join_or_total_work_retention_guarantee",
					"closed_batcher_input_or_closed_output_runtime_execution",
					"all_ones_or_other_nonfinite_Bloom_ApproximatedSize_projection",
					"Bloom_mutation_after_model_conversion_or_concurrent_Bloom_access",
					"source_node_or_raw_Bloom_database_durability",
					"BfPeers_semantics_beyond_direct_projection_to_the_leechers_column",
					"repository_retry_requeue_idempotency_or_exactly_once_delivery",
					"metric_value_as_actual_inserted_updated_or_committed_database_rows",
					"log_delivery_format_level_or_ordering",
					"live_DNS_UDP_DHT_scrape_or_upstream_worker_behavior",
					"Rust_API_worker_repository_stats_shutdown_application_wiring_deployment_or_readiness",
				},
				Evidence: "runtime rows call actual bloom.FromScrape, actual BloomFilter.ApproximatedSize through createTorrentSourceModel, and actual createTorrentSourceModel; the duplicate row executes only a source-pinned first-wins test harness; runPersistSources, persistScrapedTorrentSources, time.New, batching goroutines, metrics, logs, and PostgreSQL are never executed",
			},
		},
	}
}

func crawlerPersistSourcesRuntimeFixture(
	t *testing.T,
	id string,
	harness string,
	inputs []crawlerPersistSourcesScrape,
) crawlerPersistSourcesFixture {
	t.Helper()
	models := make([]crawlerPersistSourcesModel, 0, len(inputs))
	blooms := make([]crawlerPersistSourcesBloom, 0, len(inputs))
	calls := make([]string, 0, len(inputs))
	duplicates := make([]string, 0)
	errors := make([]string, 0)
	firstOrder := make([]string, 0, len(inputs))
	seen := make(map[protocol.ID]struct{}, len(inputs))

	for _, input := range inputs {
		value, observation := crawlerPersistSourcesBuildScrape(t, input)
		blooms = append(blooms, observation)
		if _, ok := seen[value.infoHash]; ok {
			duplicates = append(duplicates, value.infoHash.String())
			continue
		}
		seen[value.infoHash] = struct{}{}
		firstOrder = append(firstOrder, value.infoHash.String())
		calls = append(calls, value.infoHash.String())
		source, err := createTorrentSourceModel(value)
		if err != nil {
			errors = append(errors, err.Error())
			continue
		}
		models = append(models, crawlerPersistSourcesProjectModel(source))
	}

	classification := "RUNTIME_EXACT"
	composition := "actual_createTorrentSourceModel_with_actual_bloom_FromScrape_and_ApproximatedSize"
	if harness == "source_pinned_first_wins_loop_harness" {
		classification = "RUNTIME_EXACT_TEST_HARNESS"
		composition = "source_pinned_first_wins_loop_test_harness_plus_actual_createTorrentSourceModel_bloom_FromScrape_and_ApproximatedSize"
	}
	return crawlerPersistSourcesFixture{
		ID: id, Subsystem: "dht_crawler_persist_sources", Classification: classification,
		Oracle: crawlerPersistSourcesOracle{
			Composition: composition, Determinism: "pure_fixed_inputs_without_clock_database_goroutine_or_channel_execution",
			Harness: harness, Database: "not_executed", Clock: "not_read",
		},
		Input: crawlerPersistSourcesInput{Kind: harness, Scrapes: inputs},
		Expected: crawlerPersistSourcesExpected{
			Models: models, BloomObservations: blooms, ConverterCallInfoHashes: calls,
			DuplicateInfoHashes: duplicates, ConversionErrors: errors, FirstOccurrenceOrder: firstOrder,
			RunPersistSourcesExecuted: false,
		},
	}
}

func crawlerPersistSourcesScrapeInput(
	value byte,
	node string,
	seeders crawlerPersistSourcesFilterInput,
	peers crawlerPersistSourcesFilterInput,
) crawlerPersistSourcesScrape {
	return crawlerPersistSourcesScrape{
		InfoHash: crawlerPersistSourcesID(value).String(), Node: crawlerPersistSourcesProjectAddress(netip.MustParseAddrPort(node)),
		Seeders: crawlerPersistSourcesNormalizeFilterInput(seeders), Peers: crawlerPersistSourcesNormalizeFilterInput(peers),
	}
}

func crawlerPersistSourcesNormalizeFilterInput(input crawlerPersistSourcesFilterInput) crawlerPersistSourcesFilterInput {
	if input.RawIPs == nil {
		input.RawIPs = []string{}
	}
	if input.Ranges == nil {
		input.Ranges = []crawlerPersistSourcesFilterRange{}
	}
	return input
}

func crawlerPersistSourcesBuildScrape(
	t *testing.T,
	input crawlerPersistSourcesScrape,
) (infoHashWithScrape, crawlerPersistSourcesBloom) {
	t.Helper()
	infoHash := protocol.MustParseID(input.InfoHash)
	node := crawlerPersistSourcesParseAddress(input.Node)
	seedersRaw := crawlerPersistSourcesBuildRawFilter(t, input.Seeders)
	peersRaw := crawlerPersistSourcesBuildRawFilter(t, input.Peers)
	seeders := bloom.FromScrape(seedersRaw)
	peers := bloom.FromScrape(peersRaw)
	return infoHashWithScrape{
			nodeHasPeersForHash: nodeHasPeersForHash{infoHash: infoHash, node: node},
			bfsd:                seeders,
			bfpe:                peers,
		}, crawlerPersistSourcesBloom{
			InfoHash:            infoHash.String(),
			SeedersBloomSHA256:  fmt.Sprintf("%x", sha256.Sum256(seedersRaw[:])),
			PeersBloomSHA256:    fmt.Sprintf("%x", sha256.Sum256(peersRaw[:])),
			SeedersApproximated: seeders.ApproximatedSize(),
			PeersApproximated:   peers.ApproximatedSize(),
		}
}

func crawlerPersistSourcesBuildRawFilter(
	t *testing.T,
	input crawlerPersistSourcesFilterInput,
) pdht.ScrapeBloomFilter {
	t.Helper()
	var result pdht.ScrapeBloomFilter
	for _, rawHex := range input.RawIPs {
		raw, err := hex.DecodeString(rawHex)
		if err != nil {
			t.Fatalf("decode raw IP %q: %v", rawHex, err)
		}
		result.AddIP(net.IP(raw))
	}
	for _, valueRange := range input.Ranges {
		base, err := hex.DecodeString(valueRange.Base)
		if err != nil {
			t.Fatalf("decode range base %q: %v", valueRange.Base, err)
		}
		if valueRange.Count < 0 {
			t.Fatalf("negative range count %d", valueRange.Count)
		}
		for offset := range valueRange.Count {
			result.AddIP(net.IP(crawlerPersistSourcesAddBigEndian(t, base, uint64(offset))))
		}
	}
	return result
}

func crawlerPersistSourcesAddBigEndian(t *testing.T, base []byte, offset uint64) []byte {
	t.Helper()
	result := append([]byte(nil), base...)
	carry := offset
	for index := len(result) - 1; index >= 0 && carry != 0; index-- {
		sum := uint64(result[index]) + carry
		result[index] = byte(sum)
		carry = sum >> 8
	}
	if carry != 0 {
		t.Fatal("persist-sources filter range overflows address width")
	}
	return result
}

func crawlerPersistSourcesProjectModel(source model.TorrentsTorrentSource) crawlerPersistSourcesModel {
	return crawlerPersistSourcesModel{
		Source: source.Source, InfoHash: source.InfoHash.String(), Seeders: source.Seeders.Uint,
		SeedersValid: source.Seeders.Valid, Leechers: source.Leechers.Uint, LeechersValid: source.Leechers.Valid,
		SeenCount: source.SeenCount, ImportIDValid: source.ImportID.Valid, PublishedAtValid: source.PublishedAt.Valid,
		CreatedAtZero: source.CreatedAt.IsZero(), UpdatedAtZero: source.UpdatedAt.IsZero(),
		SourceNodeRetained: false, RawSeedersBloomRetained: false, RawPeersBloomRetained: false,
	}
}

func crawlerPersistSourcesID(value byte) (id protocol.ID) {
	id[19] = value
	return id
}

func crawlerPersistSourcesProjectAddress(addr netip.AddrPort) crawlerPersistSourcesAddress {
	scope := uint32(0)
	if addr.Addr().Zone() != "" {
		if _, err := fmt.Sscan(addr.Addr().Zone(), &scope); err != nil {
			panic(err)
		}
	}
	return crawlerPersistSourcesAddress{IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: scope}
}

func crawlerPersistSourcesParseAddress(value crawlerPersistSourcesAddress) netip.AddrPort {
	addr := netip.MustParseAddr(value.IP)
	if value.Scope != 0 {
		addr = addr.WithZone(fmt.Sprint(value.Scope))
	}
	return netip.AddrPortFrom(addr, value.Port)
}

func crawlerPersistSourcesOneRowSQL() string {
	return "INSERT INTO torrents_torrent_sources " +
		"(source, info_hash, seeders, leechers, published_at, seen_count, created_at, updated_at) " +
		"SELECT v.source, decode(v.info_hash, 'hex'), v.seeders, v.leechers, v.published_at, v.seen_count, v.created_at, v.updated_at FROM (VALUES " +
		"(?,?,?::integer,?::integer,?::timestamptz,?::integer,?::timestamptz,?::timestamptz)" +
		") AS v(source, info_hash, seeders, leechers, published_at, seen_count, created_at, updated_at) " +
		"WHERE EXISTS (SELECT 1 FROM torrents t WHERE t.info_hash = decode(v.info_hash, 'hex')) " +
		"ON CONFLICT (info_hash, source) DO UPDATE SET " +
		"seeders = excluded.seeders, leechers = excluded.leechers, published_at = excluded.published_at, " +
		"updated_at = excluded.updated_at, seen_count = torrents_torrent_sources.seen_count + 1"
}

func crawlerPersistSourcesNormalizedASTDigests(t *testing.T) map[string]string {
	t.Helper()
	specs := []crawlerPersistSourcesASTSpec{
		{key: "batching.NewBatchingChannel", path: "internal/concurrency/batching_channel.go", kind: "func", name: "NewBatchingChannel"},
		{key: "batching.In", path: "internal/concurrency/batching_channel.go", kind: "func", name: "In"},
		{key: "batching.Out", path: "internal/concurrency/batching_channel.go", kind: "func", name: "Out"},
		{key: "batching.batch", path: "internal/concurrency/batching_channel.go", kind: "func", name: "batch"},
		{key: "batching.flush", path: "internal/concurrency/batching_channel.go", kind: "func", name: "flush"},
		{key: "bloom.FromScrape", path: "internal/bloom/bloom.go", kind: "func", name: "FromScrape"},
		{key: "crawler.nodeHasPeersForHash", path: "internal/dhtcrawler/crawler.go", kind: "type", name: "nodeHasPeersForHash"},
		{key: "crawler.infoHashWithScrape", path: "internal/dhtcrawler/crawler.go", kind: "type", name: "infoHashWithScrape"},
		{key: "crawler.start", path: "internal/dhtcrawler/crawler.go", kind: "func", name: "start"},
		{key: "factory.New", path: "internal/dhtcrawler/factory.go", kind: "func", name: "New"},
		{key: "model.NewNullUint", path: "internal/model/null.go", kind: "func", name: "NewNullUint"},
		{key: "model.TorrentsTorrentSource", path: "internal/model/torrents_torrent_sources.gen.go", kind: "type", name: "TorrentsTorrentSource"},
		{key: "persist.runPersistSources", path: "internal/dhtcrawler/persist.go", kind: "func", name: "runPersistSources"},
		{key: "persist.persistScrapedTorrentSources", path: "internal/dhtcrawler/persist.go", kind: "func", name: "persistScrapedTorrentSources"},
		{key: "persist.createTorrentSourceModel", path: "internal/dhtcrawler/persist.go", kind: "func", name: "createTorrentSourceModel"},
		{key: "protocol.ID.String", path: "internal/protocol/id.go", kind: "func", name: "String"},
	}
	digests := make(map[string]string, len(specs))
	missing := false
	for _, specification := range specs {
		node, files := crawlerPersistSourcesFindASTNode(t, specification)
		var normalized bytes.Buffer
		if err := format.Node(&normalized, files, node); err != nil {
			t.Fatal(err)
		}
		actual := fmt.Sprintf("%x", sha256.Sum256(normalized.Bytes()))
		digests[specification.key] = actual
		expected := crawlerPersistSourcesExpectedNormalizedASTSHA256[specification.key]
		if expected == "" {
			missing = true
		} else if actual != expected {
			t.Fatalf("normalized AST SHA-256 %s = %s, want %s", specification.key, actual, expected)
		}
	}
	if missing {
		encoded, _ := json.MarshalIndent(digests, "", "  ")
		t.Fatalf("fill crawlerPersistSourcesExpectedNormalizedASTSHA256 with:\n%s", encoded)
	}
	return digests
}

func crawlerPersistSourcesFindASTNode(
	t *testing.T,
	specification crawlerPersistSourcesASTSpec,
) (ast.Node, *token.FileSet) {
	t.Helper()
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, filepath.Join(crawlerPersistSourcesRoot(t), specification.path), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		switch typed := declaration.(type) {
		case *ast.FuncDecl:
			if specification.kind == "func" && typed.Name.Name == specification.name {
				return typed, files
			}
		case *ast.GenDecl:
			if specification.kind != "type" {
				continue
			}
			for _, raw := range typed.Specs {
				if typeSpec, ok := raw.(*ast.TypeSpec); ok && typeSpec.Name.Name == specification.name {
					return typeSpec, files
				}
			}
		}
	}
	t.Fatalf("%s %s not found in %s", specification.kind, specification.name, specification.path)
	return nil, nil
}

func crawlerPersistSourcesSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	paths := []string{
		"go.mod", "go.sum",
		"internal/bloom/bloom.go", "internal/concurrency/batching_channel.go",
		"internal/dhtcrawler/config.go", "internal/dhtcrawler/crawler.go", "internal/dhtcrawler/factory.go",
		"internal/dhtcrawler/persist.go", "internal/dhtcrawler/scrape.go",
		"internal/model/null.go", "internal/model/torrents_torrent_sources.gen.go", "internal/protocol/id.go",
		"internal/protocol/dht/scrape.go", "migrations/00001_init.sql", "migrations/00017_ordering_fields.sql",
		"migrations/00025_dht_seen_count.sql",
	}
	digests := make(map[string]string, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(filepath.Join(crawlerPersistSourcesRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		digests[path] = fmt.Sprintf("%x", sha256.Sum256(contents))
	}
	return digests
}

func crawlerPersistSourcesPrerequisiteDigests(t *testing.T) map[string]string {
	t.Helper()
	want := map[string]string{
		"testdata/parity/dht/dht_crawler_scrape.jsonl": "d434306fd60678be95cabd53d59ea152f6a013bf2e486f4bb2456aa8da2c6d9b",
		"testdata/parity/dht/scrape_bloom.jsonl":       "760f868a2cb53d8342e02c84b99ec0335fa20df52d5d2695b00d3f7e2d7ac287",
	}
	for path, expected := range want {
		contents, err := os.ReadFile(filepath.Join(crawlerPersistSourcesRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		if actual := fmt.Sprintf("%x", sha256.Sum256(contents)); actual != expected {
			t.Fatalf("%s SHA-256 = %s, want %s", path, actual, expected)
		}
	}
	return want
}

func crawlerPersistSourcesDependencyLine(t *testing.T, path string, prefix string) string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(crawlerPersistSourcesRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(contents), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			return strings.TrimSpace(line)
		}
	}
	t.Fatalf("dependency line with prefix %q not found in %s", prefix, path)
	return ""
}

func crawlerPersistSourcesRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve persist-sources generator source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
}

func crawlerPersistSourcesReconcile(t *testing.T, fixtures []crawlerPersistSourcesFixture) {
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
		t.Fatal("persist-sources fixture must be nonempty LF-only JSONL with a final LF")
	}
	crawlerPersistSourcesValidateStrictJSONL(t, encoded.Bytes(), fixtures)
	actualHash := fmt.Sprintf("%x", sha256.Sum256(encoded.Bytes()))
	if crawlerPersistSourcesFixtureSHA256 != "" && actualHash != crawlerPersistSourcesFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerPersistSourcesFixtureSHA256)
	}
	path := filepath.Join(crawlerPersistSourcesRoot(t), "testdata/parity/dht/dht_crawler_persist_sources.jsonl")
	if *updateDHTCrawlerPersistSourcesParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-persist-sources-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler persist-sources fixture is stale; rerun with -update-dht-crawler-persist-sources-parity")
	}
}

func crawlerPersistSourcesValidateStrictJSONL(
	t *testing.T,
	contents []byte,
	want []crawlerPersistSourcesFixture,
) {
	t.Helper()
	scanner := bufio.NewScanner(bytes.NewReader(contents))
	decoded := make([]crawlerPersistSourcesFixture, 0, len(want))
	for scanner.Scan() {
		decoder := json.NewDecoder(strings.NewReader(scanner.Text()))
		decoder.DisallowUnknownFields()
		var fixture crawlerPersistSourcesFixture
		if err := decoder.Decode(&fixture); err != nil {
			t.Fatalf("strict decode row %d: %v", len(decoded)+1, err)
		}
		var extra json.RawMessage
		if err := decoder.Decode(&extra); err != io.EOF {
			t.Fatalf("strict decode row %d trailing JSON: %v", len(decoded)+1, err)
		}
		decoded = append(decoded, fixture)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(want) {
		t.Fatalf("strict decoded row count = %d, want %d", len(decoded), len(want))
	}
	for index := range want {
		if decoded[index].ID != want[index].ID {
			t.Fatalf("strict decoded row %d ID = %q, want %q", index+1, decoded[index].ID, want[index].ID)
		}
	}
}
