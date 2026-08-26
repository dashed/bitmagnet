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
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	ami "github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/blobmigration"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	pmetainfo "github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
)

var updateDHTCrawlerPersistTorrentsOracle = flag.Bool(
	"update-dht-crawler-persist-torrents-oracle",
	false,
	"rewrite the DHT crawler persist-torrents oracle fixture",
)

const crawlerPersistTorrentsFixtureSHA256 = "40adced4a96a860354d8ba74c412566e2a72979261bd674994c4ef18d6680bc5"

var crawlerPersistTorrentsFixtureIDs = [...]string{
	"production_source_factory_batcher_lifecycle_lookup_dedup_transaction_and_fanout_contract",
	"v1_single_default_projection_from_verified_raw_info",
	"v1_threshold_n_and_n_plus_one_save_pieces_blob_summary_matrix",
	"pure_v2_single_and_pinned_hybrid_dual_identity_file_order_matrix",
	"v2_duplicate_filter_existing_batch_same_pk_v1_and_stable_order_matrix",
	"exact_primary_key_first_wins_and_classifier_100_101_grouping_harness",
}

var crawlerPersistTorrentsExpectedNormalizedASTSHA256 = map[string]string{
	"batching.NewBatchingChannel":            "2c9a3fa894f82680a8cb8437d8dbad6d3bc2da9a7594c83553ef7650dd472dc6",
	"batching.In":                            "f5ef939724dc08bc0fa39e9fa2e0863e45acd1c965609ad91fa7082fd6632b21",
	"batching.Out":                           "f677733fd65c621331747365d30bc29503cda90a21e5aba68ece706afd5d2e3c",
	"batching.batch":                         "ebedd32544fc4a53c3cb016fd883da2e76267dd492a7c5f88ba2ebcf8232858c",
	"batching.flush":                         "3c72fb1d8c6d52bfed5b60a796d5bfee0e13da3b745c220ac01467a88de1f274",
	"blob.BuildFileSummary":                  "be962f342758d0e7f03a831d49827c8af57a4d8a3560b3c80cc716196610854c",
	"blob.DeserializeFiles":                  "d150898e585295c4eb231b6308e0477ac64da15062b468fe6f4c1cb1a5e53bd3",
	"blob.ExtractUniqueExtensions":           "adb6aaebde6329309ae9bf10485a0a2085f73a7f27ce7d2ff44389c7755f5db7",
	"blob.SerializeFiles":                    "62127451ffc61b855c9c5a2a0ffc4977f7d1ecc5b546ca9f670c13296842c1b2",
	"config.Config":                          "3883ac0fbf4869de1caa10bfe01100147e8b5e9681a65ad44c2910a79e531a73",
	"config.NewDefaultConfig":                "d044a4710817daf9a87dfab03ce22f138da3c6e1bf94d40bbbfd0fea70673f32",
	"crawler.infoHashWithMetaInfo":           "7de701e7f26b3dbbe7f82adc220ec88ffc362afd476bf5899fe20401afa0ce6d",
	"crawler.start":                          "d61a318ce626352ee4f5cd5dd48191d767bbfe45b6a9def673cd185eada4f67b",
	"factory.New":                            "0204a00fd63b275339d63d622865858571c153bc81fc738784a78e1c150fec80",
	"factory.Params":                         "265ba054222c6a3e228fb2b11e822ab994c6295a36a536531bc1c1bb4401c00a",
	"factory.Result":                         "ba6e8c3112414947f599febf6d342c19a06b91eb386e6a932a04e888523cea65",
	"metainfo.Info":                          "08928b81ea00a8adcee59959f876fcdd623a7059f580e7452f41eded1789954e",
	"metainfo.ParseMetaInfoBytes":            "4de434e83335941b1db217f8cade3a09c7c01df133555f085baf70ad616f9b8b",
	"metainfo.ParsedInfo":                    "51664f615ffbaff8382bef86eadfc7d0b1c722acfd76b2ca86705a921d3065d0",
	"model.NewQueueJob":                      "a1a890551e6feb59b062a2dd48be25758013050fa62ebf21e4b84f5772f8e25a",
	"model.QueueJob":                         "7183f138723be0f7dab2841209f85a97bfb18a6c95735b84130c5cb6f0db9285",
	"model.QueueJobDelayBy":                  "e219f5e4d4fd1382964aec2c37dcdff2528ddd962daf6fc014f73d56087c52a1",
	"model.Torrent":                          "42657deef97d08eea3bdbe92724885e367be6b2967b0cd81fef21f88e88d358a",
	"model.TorrentFile":                      "c5631b4d8156c03dfb99459791f062045f9e191a47bb23041ba5783c5cdba109",
	"model.TorrentFileSummary":               "250b98900b722c9d57a7abcc23d395fae07865e474ac10f9be7a84261ccb2620",
	"model.TorrentPieces":                    "db025620774cb761a9a58267163583454c8f1ef039ead228e0aaf7e1c0c4097c",
	"model.TorrentsTorrentSource":            "f71036cb64dfaa18994e0caa7fe63e394a93e3f29cf00312ce7f7d2e2cf358e5",
	"persist.buildTorrentFileSummary":        "8762a76dd0e409fb062b141ef9cbbf252f192e974b4dbaece52d57ef41b8b139",
	"persist.createTorrentModel":             "ec8602b3a04c724a6941c2012a1b7c4891a53828dc4f34c5cbc7f7978f646852",
	"persist.dropV2Duplicate":                "8f644dca197a59dd99b92c6f3648e2e4a31148be0db3b0aefa573c4e232a1b04",
	"persist.filterV2Duplicates":             "dde18c5742a58b6290a578c1e397bb94587a40854e0a4b0250860e922df526f5",
	"persist.lookupExistingV2":               "8e22d9abdb957e5f12a55187e834b0438ba907820160dda8d33a5542138d4b04",
	"persist.runPersistTorrents":             "fb761e3ec7c805218cc826f978352c8ebf831ab35b329ef5d868b9d8d12be199",
	"persist.torrentFileSummaryPersistQuery": "dac2d8fa8853858404ddb89cd926f70ab04a014472a815af195361b5e0f0254f",
	"processor.MessageParams":                "d440147bc9e96dac2e745fedbe4f8c1a64f5192a76bb9122d4614cccf1e78990",
	"processor.NewQueueJob":                  "5ce97c9b684c7a1f1e18afcd873a7e162cc50c8a093c0b2276a6c94dc80fa0da",
	"protocol.InfoHashV2.ToShort":            "3bc66809740dd16c9e4dfd8813d4a19667a1d9a1a353197de3f03144e68d457b",
}

type crawlerPersistTorrentsFixture struct {
	ID             string                         `json:"id"`
	Subsystem      string                         `json:"subsystem"`
	Classification string                         `json:"classification"`
	Execution      string                         `json:"execution"`
	Oracle         crawlerPersistTorrentsOracle   `json:"oracle"`
	Input          crawlerPersistTorrentsInput    `json:"input"`
	Expected       crawlerPersistTorrentsExpected `json:"expected"`
}

type crawlerPersistTorrentsOracle struct {
	Composition                string   `json:"composition"`
	Determinism                string   `json:"determinism"`
	Harness                    string   `json:"harness"`
	Database                   string   `json:"database"`
	Clock                      string   `json:"clock"`
	RunPersistTorrentsExecuted bool     `json:"runPersistTorrentsExecuted"`
	BatcherExecuted            bool     `json:"batcherExecuted"`
	DatabaseExecuted           bool     `json:"databaseExecuted"`
	ActualFunctionsExecuted    []string `json:"actualFunctionsExecuted"`
	SourcePinnedHarnessSteps   []string `json:"sourcePinnedHarnessSteps"`
}

type crawlerPersistTorrentsInput struct {
	Kind       string                                 `json:"kind"`
	Cases      []crawlerPersistTorrentsModelCaseInput `json:"cases"`
	DedupCases []crawlerPersistTorrentsDedupCaseInput `json:"dedupCases"`
	Classifier *crawlerPersistTorrentsClassifierInput `json:"classifier,omitempty"`
}

type crawlerPersistTorrentsModelCaseInput struct {
	Label              string `json:"label"`
	RawInfoHex         string `json:"rawInfoHex"`
	RawInfoSHA256      string `json:"rawInfoSha256"`
	RequestedInfoHash  string `json:"requestedInfoHash"`
	SavePieces         bool   `json:"savePieces"`
	SaveFilesThreshold uint   `json:"saveFilesThreshold"`
	SourceFixture      string `json:"sourceFixture,omitempty"`
}

type crawlerPersistTorrentsExpected struct {
	Models                     []crawlerPersistTorrentsModelResult     `json:"models"`
	DedupCases                 []crawlerPersistTorrentsDedupCaseResult `json:"dedupCases"`
	Classifier                 *crawlerPersistTorrentsClassifierResult `json:"classifier,omitempty"`
	Source                     *crawlerPersistTorrentsSource           `json:"source,omitempty"`
	RunPersistTorrentsExecuted bool                                    `json:"runPersistTorrentsExecuted"`
}

type crawlerPersistTorrentsModelResult struct {
	Label                               string                              `json:"label"`
	ParseError                          string                              `json:"parseError"`
	CreateError                         string                              `json:"createError"`
	InfoHash                            string                              `json:"infoHash"`
	InfoHashV1                          string                              `json:"infoHashV1"`
	InfoHashV2                          string                              `json:"infoHashV2"`
	MetaVersion                         uint16                              `json:"metaVersion"`
	MetaVersionValid                    bool                                `json:"metaVersionValid"`
	Name                                string                              `json:"name"`
	Size                                uint                                `json:"size"`
	Private                             bool                                `json:"private"`
	FilesStatus                         string                              `json:"filesStatus"`
	FilesCount                          uint                                `json:"filesCount"`
	FilesCountValid                     bool                                `json:"filesCountValid"`
	Files                               []crawlerPersistTorrentsFile        `json:"files"`
	FilesNil                            bool                                `json:"filesNil"`
	FilesDataPresent                    bool                                `json:"filesDataPresent"`
	FilesDataNil                        bool                                `json:"filesDataNil"`
	FilesDataByteLength                 int                                 `json:"filesDataByteLength"`
	FilesDataSHA256                     string                              `json:"filesDataSha256"`
	DecodedFiles                        []crawlerPersistTorrentsFile        `json:"decodedFiles"`
	DecodedFilesNil                     bool                                `json:"decodedFilesNil"`
	DecodedFilesMatchRetainedCoreFields bool                                `json:"decodedFilesMatchRetainedCoreFields"`
	FileExtensions                      []string                            `json:"fileExtensions"`
	FileExtensionsNil                   bool                                `json:"fileExtensionsNil"`
	Sources                             []crawlerPersistTorrentsSourceModel `json:"sources"`
	SourcesNil                          bool                                `json:"sourcesNil"`
	Pieces                              crawlerPersistTorrentsPieces        `json:"pieces"`
	Summary                             *crawlerPersistTorrentsSummary      `json:"summary,omitempty"`
}

type crawlerPersistTorrentsFile struct {
	Index          uint   `json:"index"`
	Path           string `json:"path"`
	Size           uint   `json:"size"`
	Extension      string `json:"extension"`
	ExtensionValid bool   `json:"extensionValid"`
}

type crawlerPersistTorrentsSourceModel struct {
	Source   string `json:"source"`
	InfoHash string `json:"infoHash"`
}

type crawlerPersistTorrentsPieces struct {
	Present     bool   `json:"present"`
	InfoHash    string `json:"infoHash"`
	PieceLength int64  `json:"pieceLength"`
	PiecesHex   string `json:"piecesHex"`
}

type crawlerPersistTorrentsSummary struct {
	InfoHash                        string   `json:"infoHash"`
	FileCount                       int      `json:"fileCount"`
	TotalSize                       int64    `json:"totalSize"`
	LargestFileSize                 int64    `json:"largestFileSize"`
	Extensions                      []string `json:"extensions"`
	HasVideo                        bool     `json:"hasVideo"`
	HasSubtitle                     bool     `json:"hasSubtitle"`
	HasAudio                        bool     `json:"hasAudio"`
	CompressedBytesValid            bool     `json:"compressedBytesValid"`
	CompressedBytes                 int      `json:"compressedBytes"`
	CompressedBytesMatchesFilesData bool     `json:"compressedBytesMatchesFilesData"`
	CreatedAt                       string   `json:"createdAt"`
	UpdatedAt                       string   `json:"updatedAt"`
}

type crawlerPersistTorrentsDedupCaseInput struct {
	Label    string                             `json:"label"`
	Items    []crawlerPersistTorrentsDedupItem  `json:"items"`
	Existing []crawlerPersistTorrentsExistingV2 `json:"existing"`
}

type crawlerPersistTorrentsDedupItem struct {
	PrimaryInfoHash string `json:"primaryInfoHash"`
	InfoHashV2      string `json:"infoHashV2"`
}

type crawlerPersistTorrentsExistingV2 struct {
	InfoHashV2      string `json:"infoHashV2"`
	PrimaryInfoHash string `json:"primaryInfoHash"`
}

type crawlerPersistTorrentsDedupCaseResult struct {
	Label                 string   `json:"label"`
	KeptPrimaryInfoHashes []string `json:"keptPrimaryInfoHashes"`
	Dropped               int      `json:"dropped"`
}

type crawlerPersistTorrentsClassifierInput struct {
	UniqueCount       int    `json:"uniqueCount"`
	ClassifyBatchSize int    `json:"classifyBatchSize"`
	DuplicateInfoHash string `json:"duplicateInfoHash"`
	FirstMarker       string `json:"firstMarker"`
	LaterMarker       string `json:"laterMarker"`
}

type crawlerPersistTorrentsClassifierResult struct {
	InputCount            int                              `json:"inputCount"`
	UniqueCount           int                              `json:"uniqueCount"`
	DuplicateInfoHashes   []string                         `json:"duplicateInfoHashes"`
	DuplicateWinnerMarker string                           `json:"duplicateWinnerMarker"`
	ClassifierGroups      [][]string                       `json:"classifierGroups"`
	QueueJobs             []crawlerPersistTorrentsQueueJob `json:"queueJobs"`
}

type crawlerPersistTorrentsQueueJob struct {
	Queue                       string `json:"queue"`
	Payload                     string `json:"payload"`
	Fingerprint                 string `json:"fingerprint"`
	Status                      string `json:"status"`
	Retries                     uint   `json:"retries"`
	MaxRetries                  uint   `json:"maxRetries"`
	Priority                    int    `json:"priority"`
	ArchivalDurationNanoseconds int64  `json:"archivalDurationNanoseconds"`
	DelayMillis                 int64  `json:"delayMillis"`
	AbsoluteRunAfterExcluded    bool   `json:"absoluteRunAfterExcluded"`
}

type crawlerPersistTorrentsSource struct {
	Factory              crawlerPersistTorrentsFactoryContract     `json:"factory"`
	Batcher              crawlerPersistTorrentsBatcherContract     `json:"batcher"`
	Lifecycle            crawlerPersistTorrentsLifecycleContract   `json:"lifecycle"`
	Worker               crawlerPersistTorrentsWorkerContract      `json:"worker"`
	LookupDedup          crawlerPersistTorrentsLookupDedupContract `json:"lookupDedup"`
	Transactions         []crawlerPersistTorrentsTransactionTable  `json:"transactions"`
	SchemaConstraints    []crawlerPersistTorrentsSchemaConstraint  `json:"schemaConstraints"`
	SeededTorrentSources []crawlerPersistTorrentsSeededSource      `json:"seededTorrentSources"`
	Fanout               crawlerPersistTorrentsFanoutContract      `json:"fanout"`
	Dependencies         []string                                  `json:"dependencies"`
	NormalizedASTSHA256  map[string]string                         `json:"normalizedAstSha256"`
	SourceSHA256         map[string]string                         `json:"sourceSha256"`
	PrerequisiteSHA256   map[string]string                         `json:"prerequisiteSha256"`
	Nonclaims            []string                                  `json:"nonclaims"`
	Evidence             string                                    `json:"evidence"`
}

type crawlerPersistTorrentsSeededSource struct {
	Key             string `json:"key"`
	Name            string `json:"name"`
	SourceMigration string `json:"sourceMigration"`
}

type crawlerPersistTorrentsSchemaConstraint struct {
	Table           string   `json:"table"`
	Kind            string   `json:"kind"`
	Columns         []string `json:"columns"`
	Predicate       string   `json:"predicate"`
	References      string   `json:"references"`
	Expression      string   `json:"expression"`
	Unique          bool     `json:"unique"`
	SourceMigration string   `json:"sourceMigration"`
}

type crawlerPersistTorrentsFactoryContract struct {
	InputCapacity             int      `json:"inputCapacity"`
	MaximumBatchSize          int      `json:"maximumBatchSize"`
	BatchIntervalMillis       int      `json:"batchIntervalMillis"`
	OutputCapacity            int      `json:"outputCapacity"`
	ConfigurationFields       []string `json:"configurationFields"`
	DefaultSaveFilesThreshold uint     `json:"defaultSaveFilesThreshold"`
	DefaultSavePieces         bool     `json:"defaultSavePieces"`
	Hardcoded                 bool     `json:"hardcoded"`
}

type crawlerPersistTorrentsBatcherContract struct {
	FlushAtMaximumSize             bool   `json:"flushAtMaximumSize"`
	FlushOnNonemptyTicker          bool   `json:"flushOnNonemptyTicker"`
	TickerStartsAtConstruction     bool   `json:"tickerStartsAtConstruction"`
	TickerResetsAfterFlush         bool   `json:"tickerResetsAfterFlush"`
	FlushBlocksOnOutput            bool   `json:"flushBlocksOnOutput"`
	ContextAware                   bool   `json:"contextAware"`
	InputCloseExitsLoop            bool   `json:"inputCloseExitsLoop"`
	ClosedInputSourceOutcome       string `json:"closedInputSourceOutcome"`
	OutputReceiveChecksOpenBoolean bool   `json:"outputReceiveChecksOpenBoolean"`
}

type crawlerPersistTorrentsLifecycleContract struct {
	FactoryStartsCrawlerDetached       bool `json:"factoryStartsCrawlerDetached"`
	CrawlerStartsPersistWorkerDetached bool `json:"crawlerStartsPersistWorkerDetached"`
	CrawlerWaitsOnlyForStopped         bool `json:"crawlerWaitsOnlyForStopped"`
	SharedContextCancelledAfterStopped bool `json:"sharedContextCancelledAfterStopped"`
	StopClosesBatcherInput             bool `json:"stopClosesBatcherInput"`
	StopDrainsPersistTorrents          bool `json:"stopDrainsPersistTorrents"`
	StopJoinsWorkerOrBatcher           bool `json:"stopJoinsWorkerOrBatcher"`
}

type crawlerPersistTorrentsWorkerContract struct {
	V2LookupPrecedesPrimaryDedup          bool   `json:"v2LookupPrecedesPrimaryDedup"`
	ExactPrimaryKeyFirstOccurrenceWins    bool   `json:"exactPrimaryKeyFirstOccurrenceWins"`
	PrimaryKeyInsertedBeforeConversion    bool   `json:"primaryKeyInsertedBeforeConversion"`
	ConversionErrorLoggedAndSkipped       bool   `json:"conversionErrorLoggedAndSkipped"`
	ClassifierBatchSize                   int    `json:"classifierBatchSize"`
	ClassifierOrder                       string `json:"classifierOrder"`
	ClassifierIncludesConvertedUniqueOnly bool   `json:"classifierIncludesConvertedUniqueOnly"`
	TransactionCalledOncePerReceivedBatch bool   `json:"transactionCalledOncePerReceivedBatch"`
	MetricEntity                          string `json:"metricEntity"`
	MetricCountsPreparedTorrentModels     bool   `json:"metricCountsPreparedTorrentModels"`
	MetricIncrementedOnlyOnTransactionOK  bool   `json:"metricIncrementedOnlyOnTransactionOk"`
	RuntimeUintBits                       int    `json:"runtimeUintBits"`
	FixtureLengthsCheckedBeforeConversion bool   `json:"fixtureLengthsCheckedBeforeConversion"`
}

type crawlerPersistTorrentsLookupDedupContract struct {
	LookupFullV2Only              bool   `json:"lookupFullV2Only"`
	UniqueLookupSet               bool   `json:"uniqueLookupSet"`
	LookupChunkSize               int    `json:"lookupChunkSize"`
	LookupOrder                   string `json:"lookupOrder"`
	LookupErrorOutcome            string `json:"lookupErrorOutcome"`
	ExistingDifferentPrimaryDrops bool   `json:"existingDifferentPrimaryDrops"`
	BatchDifferentPrimaryDrops    bool   `json:"batchDifferentPrimaryDrops"`
	SamePrimaryKept               bool   `json:"samePrimaryKept"`
	V1WithoutV2Kept               bool   `json:"v1WithoutV2Kept"`
	FirstV2PrimaryWinsWithinBatch bool   `json:"firstV2PrimaryWinsWithinBatch"`
	DatabaseV2IndexUnique         bool   `json:"databaseV2IndexUnique"`
}

type crawlerPersistTorrentsTransactionTable struct {
	Order          int      `json:"order"`
	Table          string   `json:"table"`
	Conditional    string   `json:"conditional"`
	ChunkSize      int      `json:"chunkSize"`
	ConflictTarget []string `json:"conflictTarget"`
	ConflictAction string   `json:"conflictAction"`
	UpdatedColumns []string `json:"updatedColumns"`
}

type crawlerPersistTorrentsFanoutContract struct {
	OneExplicitTransaction                  bool   `json:"oneExplicitTransaction"`
	OneWallClockSampleBeforeModelLoop       bool   `json:"oneWallClockSampleBeforeModelLoop"`
	FileSummaryUsesSameWallClockSample      bool   `json:"fileSummaryUsesSameWallClockSample"`
	ScrapeOnlyAfterTransactionSuccess       bool   `json:"scrapeOnlyAfterTransactionSuccess"`
	ScrapeValuesFromPrimaryHashMap          bool   `json:"scrapeValuesFromPrimaryHashMap"`
	ScrapeOrder                             string `json:"scrapeOrder"`
	ScrapeSendContextAware                  bool   `json:"scrapeSendContextAware"`
	V2DuplicateMetricBeforeTransaction      bool   `json:"v2DuplicateMetricBeforeTransaction"`
	QueueJobDelayMillis                     int    `json:"queueJobDelayMillis"`
	QueueGroupsContainUniqueConvertedHashes bool   `json:"queueGroupsContainUniqueConvertedHashes"`
	QueueConflictRollsBackTransaction       bool   `json:"queueConflictRollsBackTransaction"`
	TransactionRetry                        bool   `json:"transactionRetry"`
	MetricsOrScrapeOnTransactionError       bool   `json:"metricsOrScrapeOnTransactionError"`
}

type crawlerPersistTorrentsASTSpec struct {
	key      string
	path     string
	kind     string
	name     string
	receiver string
}

func TestGenerateDHTCrawlerPersistTorrentsOracle(t *testing.T) {
	fixtures := []crawlerPersistTorrentsFixture{
		crawlerPersistTorrentsSourceFixture(t),
		crawlerPersistTorrentsV1SingleFixture(t),
		crawlerPersistTorrentsThresholdFixture(t),
		crawlerPersistTorrentsV2Fixture(t),
		crawlerPersistTorrentsDedupFixture(t),
		crawlerPersistTorrentsClassifierFixture(t),
	}

	wantClassifications := [...]string{
		"SOURCE_ONLY",
		"RUNTIME_EXACT",
		"RUNTIME_EXACT",
		"RUNTIME_EXACT",
		"RUNTIME_EXACT",
		"RUNTIME_EXACT_TEST_HARNESS",
	}
	wantExecutions := [...]string{
		"SOURCE_ONLY_NO_RUNTIME_OR_DATABASE_EXECUTION",
		"GO_ACTUAL_PARSE_META_INFO_BYTES_AND_CREATE_TORRENT_MODEL",
		"GO_ACTUAL_PARSE_MODEL_BLOB_DECODE_SUMMARY_AND_PIECES_MATRIX",
		"GO_ACTUAL_PARSE_AND_MODEL_PURE_V2_AND_HYBRID_MATRIX",
		"GO_ACTUAL_FILTER_V2_DUPLICATES_MATRIX",
		"SOURCE_PINNED_PRIMARY_DEDUP_AND_CLASSIFIER_GROUPING_HARNESS_ONLY",
	}
	if len(fixtures) != len(crawlerPersistTorrentsFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerPersistTorrentsFixtureIDs))
	}
	for index := range fixtures {
		if fixtures[index].ID != crawlerPersistTorrentsFixtureIDs[index] {
			t.Fatalf("fixture %d ID = %q, want %q", index+1, fixtures[index].ID, crawlerPersistTorrentsFixtureIDs[index])
		}
		if fixtures[index].Classification != wantClassifications[index] {
			t.Fatalf("fixture %d classification = %q, want %q", index+1, fixtures[index].Classification, wantClassifications[index])
		}
		if fixtures[index].Execution != wantExecutions[index] {
			t.Fatalf("fixture %d execution = %q, want %q", index+1, fixtures[index].Execution, wantExecutions[index])
		}
		if fixtures[index].Oracle.RunPersistTorrentsExecuted || fixtures[index].Oracle.BatcherExecuted || fixtures[index].Oracle.DatabaseExecuted || fixtures[index].Expected.RunPersistTorrentsExecuted {
			t.Fatalf("fixture %d unexpectedly claims runPersistTorrents, batcher, or database execution", index+1)
		}
		if fixtures[index].Oracle.ActualFunctionsExecuted == nil || fixtures[index].Oracle.SourcePinnedHarnessSteps == nil || fixtures[index].Input.Cases == nil || fixtures[index].Input.DedupCases == nil || fixtures[index].Expected.Models == nil || fixtures[index].Expected.DedupCases == nil {
			t.Fatalf("fixture %d has a nil required explicit array", index+1)
		}
	}
	crawlerPersistTorrentsReconcile(t, fixtures)
}

func crawlerPersistTorrentsSourceFixture(t *testing.T) crawlerPersistTorrentsFixture {
	t.Helper()
	return crawlerPersistTorrentsFixture{
		ID: crawlerPersistTorrentsFixtureIDs[0], Subsystem: "dht_crawler_persist_torrents",
		Classification: "SOURCE_ONLY", Execution: "SOURCE_ONLY_NO_RUNTIME_OR_DATABASE_EXECUTION",
		Oracle: crawlerPersistTorrentsOracle{
			Composition: "production_source_factory_batcher_lifecycle_lookup_dedup_transaction_and_fanout_contract",
			Determinism: "exact_normalized_AST_full_source_prerequisite_and_dependency_freshness",
			Harness:     "source_inspection_only", Database: "not_executed", Clock: "not_read",
			RunPersistTorrentsExecuted: false, BatcherExecuted: false, DatabaseExecuted: false,
			ActualFunctionsExecuted: []string{},
			SourcePinnedHarnessSteps: []string{
				"parse_and_format_named_production_AST_nodes",
				"hash_full_source_and_prerequisite_bytes",
				"extract_exact_go_mod_dependency_lines",
			},
		},
		Input: crawlerPersistTorrentsInput{Kind: "production_source_contract", Cases: []crawlerPersistTorrentsModelCaseInput{}, DedupCases: []crawlerPersistTorrentsDedupCaseInput{}},
		Expected: crawlerPersistTorrentsExpected{
			Models: []crawlerPersistTorrentsModelResult{}, DedupCases: []crawlerPersistTorrentsDedupCaseResult{},
			RunPersistTorrentsExecuted: false,
			Source: &crawlerPersistTorrentsSource{
				Factory: crawlerPersistTorrentsFactoryContract{
					InputCapacity: 1000, MaximumBatchSize: 1000, BatchIntervalMillis: 60_000, OutputCapacity: 1,
					ConfigurationFields:       []string{"saveFilesThreshold", "savePieces"},
					DefaultSaveFilesThreshold: 100, DefaultSavePieces: false, Hardcoded: true,
				},
				Batcher: crawlerPersistTorrentsBatcherContract{
					FlushAtMaximumSize: true, FlushOnNonemptyTicker: true, TickerStartsAtConstruction: true,
					TickerResetsAfterFlush: true, FlushBlocksOnOutput: true, ContextAware: false,
					InputCloseExitsLoop: false, ClosedInputSourceOutcome: "closed_input_busy_loop_without_output_close",
					OutputReceiveChecksOpenBoolean: false,
				},
				Lifecycle: crawlerPersistTorrentsLifecycleContract{
					FactoryStartsCrawlerDetached: true, CrawlerStartsPersistWorkerDetached: true,
					CrawlerWaitsOnlyForStopped: true, SharedContextCancelledAfterStopped: true,
					StopClosesBatcherInput: false, StopDrainsPersistTorrents: false, StopJoinsWorkerOrBatcher: false,
				},
				Worker: crawlerPersistTorrentsWorkerContract{
					V2LookupPrecedesPrimaryDedup: true, ExactPrimaryKeyFirstOccurrenceWins: true,
					PrimaryKeyInsertedBeforeConversion: true, ConversionErrorLoggedAndSkipped: true,
					ClassifierBatchSize: classifyBatchSize, ClassifierOrder: "kept_input_first_occurrence_order",
					ClassifierIncludesConvertedUniqueOnly: true, TransactionCalledOncePerReceivedBatch: true,
					MetricEntity: "Torrent", MetricCountsPreparedTorrentModels: true,
					MetricIncrementedOnlyOnTransactionOK: true, RuntimeUintBits: strconv.IntSize,
					FixtureLengthsCheckedBeforeConversion: true,
				},
				LookupDedup: crawlerPersistTorrentsLookupDedupContract{
					LookupFullV2Only: true, UniqueLookupSet: true, LookupChunkSize: v2LookupChunkSize,
					LookupOrder: "unspecified_Go_map_iteration_order", LookupErrorOutcome: "log_and_fail_open_with_partial_results",
					ExistingDifferentPrimaryDrops: true, BatchDifferentPrimaryDrops: true, SamePrimaryKept: true,
					V1WithoutV2Kept: true, FirstV2PrimaryWinsWithinBatch: true, DatabaseV2IndexUnique: false,
				},
				Transactions: []crawlerPersistTorrentsTransactionTable{
					{Order: 1, Table: "torrents", Conditional: "always_called", ChunkSize: 100, ConflictTarget: []string{"info_hash"}, ConflictAction: "update", UpdatedColumns: []string{"name", "files_status", "files_count", "updated_at", "files_data", "file_extensions"}},
					{Order: 2, Table: "torrent_files", Conditional: "only_when_nonempty", ChunkSize: 100, ConflictTarget: []string{}, ConflictAction: "do_nothing", UpdatedColumns: []string{}},
					{Order: 3, Table: "torrent_file_summary", Conditional: "only_when_nonempty", ChunkSize: 100, ConflictTarget: []string{"info_hash"}, ConflictAction: "update", UpdatedColumns: []string{"file_count", "total_size", "largest_file_size", "extensions", "has_video", "has_subtitle", "has_audio", "compressed_bytes", "updated_at"}},
					{Order: 4, Table: "torrents_torrent_sources", Conditional: "always_called", ChunkSize: 100, ConflictTarget: []string{}, ConflictAction: "do_nothing", UpdatedColumns: []string{}},
					{Order: 5, Table: "torrent_pieces", Conditional: "only_when_savePieces", ChunkSize: 10, ConflictTarget: []string{}, ConflictAction: "do_nothing", UpdatedColumns: []string{}},
					{Order: 6, Table: "queue_jobs", Conditional: "always_called", ChunkSize: 10, ConflictTarget: []string{}, ConflictAction: "gorm_default_insert", UpdatedColumns: []string{}},
				},
				SchemaConstraints: []crawlerPersistTorrentsSchemaConstraint{
					{Table: "torrents", Kind: "primary_key", Columns: []string{"info_hash"}, Predicate: "", Unique: true, SourceMigration: "migrations/00001_init.sql"},
					{Table: "torrents", Kind: "plain_index", Columns: []string{"info_hash_v2"}, Predicate: "", Unique: false, SourceMigration: "migrations/00023_v2_infohash.sql"},
					{Table: "torrents", Kind: "nullable_columns", Columns: []string{"info_hash_v1", "info_hash_v2", "meta_version"}, Unique: false, SourceMigration: "migrations/00023_v2_infohash.sql"},
					{Table: "torrent_files", Kind: "primary_key", Columns: []string{"info_hash", "path"}, Predicate: "", Unique: true, SourceMigration: "migrations/00001_init.sql"},
					{Table: "torrent_files", Kind: "unique_constraint", Columns: []string{"info_hash", "index"}, Predicate: "", Unique: true, SourceMigration: "migrations/00001_init.sql"},
					{Table: "torrent_files", Kind: "foreign_key", Columns: []string{"info_hash"}, References: "torrents(info_hash) ON DELETE CASCADE", Unique: false, SourceMigration: "migrations/00001_init.sql"},
					{Table: "torrent_files", Kind: "generated_column", Columns: []string{"extension"}, Expression: "substring(lower(path) from '[^/.]\\.([a-z0-9]+)$')", Unique: false, SourceMigration: "migrations/00001_init.sql"},
					{Table: "torrent_file_summary", Kind: "primary_key", Columns: []string{"info_hash"}, Predicate: "", Unique: true, SourceMigration: "migrations/00021_blob_storage.sql"},
					{Table: "torrent_file_summary", Kind: "foreign_key", Columns: []string{"info_hash"}, References: "torrents(info_hash) ON DELETE CASCADE", Unique: false, SourceMigration: "migrations/00021_blob_storage.sql"},
					{Table: "torrent_file_summary", Kind: "nullable_columns", Columns: []string{"compressed_bytes"}, Unique: false, SourceMigration: "migrations/00026_summary_compressed_bytes.sql"},
					{Table: "torrents_torrent_sources", Kind: "primary_key", Columns: []string{"source", "info_hash"}, Predicate: "", Unique: true, SourceMigration: "migrations/00001_init.sql"},
					{Table: "torrents_torrent_sources", Kind: "foreign_key", Columns: []string{"info_hash"}, References: "torrents(info_hash) ON DELETE CASCADE", Unique: false, SourceMigration: "migrations/00001_init.sql"},
					{Table: "torrents_torrent_sources", Kind: "foreign_key", Columns: []string{"source"}, References: "torrent_sources(key) ON DELETE CASCADE", Unique: false, SourceMigration: "migrations/00001_init.sql"},
					{Table: "torrent_pieces", Kind: "primary_key", Columns: []string{"info_hash"}, Predicate: "", Unique: true, SourceMigration: "migrations/00013_torrent_pieces.sql"},
					{Table: "torrent_pieces", Kind: "foreign_key", Columns: []string{"info_hash"}, References: "torrents(info_hash) ON DELETE CASCADE", Unique: false, SourceMigration: "migrations/00013_torrent_pieces.sql"},
					{Table: "queue_jobs", Kind: "primary_key", Columns: []string{"id"}, Predicate: "", Unique: true, SourceMigration: "migrations/00012_queue.sql"},
					{Table: "queue_jobs", Kind: "not_null_columns", Columns: []string{"fingerprint", "status"}, Unique: false, SourceMigration: "migrations/00012_queue.sql"},
					{Table: "queue_jobs", Kind: "partial_unique_index", Columns: []string{"fingerprint"}, Predicate: "status IN ('pending', 'retry')", Unique: true, SourceMigration: "migrations/00019_queue_fix_duplicate_key.sql"},
				},
				SeededTorrentSources: []crawlerPersistTorrentsSeededSource{
					{Key: "dht", Name: "DHT", SourceMigration: "migrations/00001_init.sql"},
				},
				Fanout: crawlerPersistTorrentsFanoutContract{
					OneExplicitTransaction: true, OneWallClockSampleBeforeModelLoop: true,
					FileSummaryUsesSameWallClockSample: true, ScrapeOnlyAfterTransactionSuccess: true,
					ScrapeValuesFromPrimaryHashMap: true, ScrapeOrder: "unspecified_Go_map_iteration_order",
					ScrapeSendContextAware: true, V2DuplicateMetricBeforeTransaction: true,
					QueueJobDelayMillis: 60_000, QueueGroupsContainUniqueConvertedHashes: true,
					QueueConflictRollsBackTransaction: true, TransactionRetry: false,
					MetricsOrScrapeOnTransactionError: false,
				},
				Dependencies:        crawlerPersistTorrentsDependencyLines(t),
				NormalizedASTSHA256: crawlerPersistTorrentsNormalizedASTDigests(t),
				SourceSHA256:        crawlerPersistTorrentsSourceDigests(t),
				PrerequisiteSHA256:  crawlerPersistTorrentsPrerequisiteDigests(t),
				Nonclaims: []string{
					"runPersistTorrents_batch_receive_loop_or_any_database_transaction_execution",
					"batching_goroutine_ticker_timing_ready_select_winner_or_closed_channel_runtime_behavior",
					"exact_GORM_generated_SQL_bind_order_rows_affected_commit_or_rollback_behavior",
					"live_PostgreSQL_schema_permissions_constraints_triggers_or_transaction_atomicity",
					"lookupExistingV2_live_query_chunk_bind_order_partial_error_or_database_contents",
					"scrape_fanout_Go_map_iteration_order_delivery_or_downstream_execution",
					"time_Now_wall_clock_values_queue_run_after_or_timestamp_equality_across_processes",
					"cross_language_outer_ZSTD_byte_or_length_equality_and_cross_version_stability",
					"live_queue_job_unique_index_conflict_or_classifier_execution",
					"Prometheus_log_delivery_or_actual_inserted_updated_committed_row_counts",
					"context_cancellation_shutdown_drain_join_retry_requeue_or_exactly_once_delivery",
					"createTorrentModel_error_path_from_current_valid_runtime_rows",
					"production_rejection_of_negative_or_uint_overflow_file_lengths_beyond_the_fixture_harness_checks",
					"synthetic_pure_v2_pieces_root_content_merkle_correctness_beyond_32_byte_structural_shape",
					"Rust_worker_repository_application_supervisor_deployment_or_readiness",
				},
				Evidence: "runtime rows execute only their ordered named parser/model/blob/summary/dedup/queue-constructor functions; the classifier row combines a source-pinned loop harness with actual queue constructors; runPersistTorrents, batching goroutines, lookupExistingV2, GORM, PostgreSQL, metrics, logs, and scrape fanout are not executed",
			},
		},
	}
}

func crawlerPersistTorrentsV1SingleFixture(t *testing.T) crawlerPersistTorrentsFixture {
	t.Helper()
	info := ami.Info{Name: "synthetic-single.bin", PieceLength: 32_768, Length: 4_096, Pieces: make([]byte, 20)}
	input := crawlerPersistTorrentsModelInput(t, "v1_single_default", info, false, 100, "")
	return crawlerPersistTorrentsRuntimeModelFixture(
		t, crawlerPersistTorrentsFixtureIDs[1],
		"actual_ParseMetaInfoBytes_then_actual_createTorrentModel_v1_single_default",
		"single_synthetic_bencoded_info_with_derived_v1_hash", []crawlerPersistTorrentsModelCaseInput{input}, nil,
	)
}

func crawlerPersistTorrentsThresholdFixture(t *testing.T) crawlerPersistTorrentsFixture {
	t.Helper()
	files := []ami.FileInfo{
		{Length: 1_000, Path: []string{"media", "video.mkv"}},
		{Length: 200, Path: []string{"media", "subs.srt"}},
		{Length: 300, Path: []string{"media", "audio.mp3"}},
		{Length: 50, Path: []string{"docs", "readme.txt"}},
	}
	pieceBytes := make([]byte, 40)
	for index := range pieceBytes {
		pieceBytes[index] = byte(index)
	}
	exact := ami.Info{Name: "threshold-exact", PieceLength: 32_768, Files: append([]ami.FileInfo(nil), files[:3]...), Pieces: pieceBytes}
	over := ami.Info{Name: "threshold-over", PieceLength: 32_768, Files: files, Pieces: pieceBytes}
	inputs := []crawlerPersistTorrentsModelCaseInput{
		crawlerPersistTorrentsModelInput(t, "exactly_n_files", exact, true, 3, ""),
		crawlerPersistTorrentsModelInput(t, "n_plus_one_files", over, true, 3, ""),
	}
	now := time.Unix(1_800_000_000, 123_456_789).UTC()
	return crawlerPersistTorrentsRuntimeModelFixture(
		t, crawlerPersistTorrentsFixtureIDs[2],
		"actual_ParseMetaInfoBytes_createTorrentModel_DeserializeFiles_and_buildTorrentFileSummary_threshold_matrix",
		"fixed_v1_exactly_N_and_N_plus_one_inputs_fixed_summary_clock", inputs, &now,
	)
}

func crawlerPersistTorrentsV2Fixture(t *testing.T) crawlerPersistTorrentsFixture {
	t.Helper()
	pureInput := crawlerPersistTorrentsPureV2SingleInput(t)
	hybridPath := "internal/dhtcrawler/testdata/bittorrent-v2-hybrid-test.torrent"
	hybridRaw := crawlerPersistTorrentsLoadInfoBytes(t, hybridPath)
	hybridInput := crawlerPersistTorrentsRawModelInput(
		"pinned_hybrid_discovered_by_v1", hybridRaw, crawlerPersistTorrentsV1Hash(hybridRaw), false, 1000, hybridPath,
	)
	return crawlerPersistTorrentsRuntimeModelFixture(
		t, crawlerPersistTorrentsFixtureIDs[3],
		"actual_ParseMetaInfoBytes_then_actual_createTorrentModel_pure_v2_single_and_pinned_hybrid",
		"synthetic_pure_v2_plus_SHA_pinned_repository_hybrid_info_bytes", []crawlerPersistTorrentsModelCaseInput{pureInput, hybridInput}, nil,
	)
}

func crawlerPersistTorrentsPureV2SingleInput(t *testing.T) crawlerPersistTorrentsModelCaseInput {
	t.Helper()
	properties, err := bencode.Marshal(ami.FileTreeFile{
		Length: 1_500_000_000, PiecesRoot: strings.Repeat("\x11", 32),
	})
	if err != nil {
		t.Fatalf("marshal pure-v2 file properties: %v", err)
	}
	leaf, err := bencode.Marshal(map[string]bencode.Bytes{"": properties})
	if err != nil {
		t.Fatalf("marshal pure-v2 file leaf: %v", err)
	}
	tree, err := bencode.Marshal(map[string]bencode.Bytes{"movie.mkv": leaf})
	if err != nil {
		t.Fatalf("marshal pure-v2 file tree: %v", err)
	}
	raw, err := bencode.Marshal(struct {
		FileTree    bencode.Bytes `bencode:"file tree"`
		MetaVersion int64         `bencode:"meta version"`
		Name        string        `bencode:"name"`
		PieceLength int64         `bencode:"piece length"`
	}{
		FileTree: tree, MetaVersion: 2, Name: "movie.mkv", PieceLength: 256 * 1024,
	})
	if err != nil {
		t.Fatalf("marshal pure-v2 info: %v", err)
	}
	return crawlerPersistTorrentsRawModelInput(
		"pure_v2_top_level_single", raw, crawlerPersistTorrentsV2ShortHash(raw), false, 1000, "synthetic_structurally_valid_BEP52_bencode",
	)
}

func crawlerPersistTorrentsDedupFixture(t *testing.T) crawlerPersistTorrentsFixture {
	t.Helper()
	v2a := crawlerPersistTorrentsV2Hex(0xaa)
	v2b := crawlerPersistTorrentsV2Hex(0xbb)
	pk1 := crawlerPersistTorrentsIDHex(1)
	pk2 := crawlerPersistTorrentsIDHex(2)
	pk3 := crawlerPersistTorrentsIDHex(3)
	pk4 := crawlerPersistTorrentsIDHex(4)
	inputs := []crawlerPersistTorrentsDedupCaseInput{
		{Label: "existing_cross_primary_drops", Items: []crawlerPersistTorrentsDedupItem{{PrimaryInfoHash: pk2, InfoHashV2: v2a}}, Existing: []crawlerPersistTorrentsExistingV2{{InfoHashV2: v2a, PrimaryInfoHash: pk1}}},
		{Label: "existing_same_primary_kept", Items: []crawlerPersistTorrentsDedupItem{{PrimaryInfoHash: pk1, InfoHashV2: v2a}}, Existing: []crawlerPersistTorrentsExistingV2{{InfoHashV2: v2a, PrimaryInfoHash: pk1}}},
		{Label: "batch_first_v2_primary_wins_stable_order", Items: []crawlerPersistTorrentsDedupItem{{PrimaryInfoHash: pk1, InfoHashV2: v2a}, {PrimaryInfoHash: pk2, InfoHashV2: v2a}, {PrimaryInfoHash: pk3, InfoHashV2: v2b}, {PrimaryInfoHash: pk4, InfoHashV2: ""}}, Existing: []crawlerPersistTorrentsExistingV2{}},
		{Label: "same_primary_rediscovery_kept", Items: []crawlerPersistTorrentsDedupItem{{PrimaryInfoHash: pk1, InfoHashV2: v2a}, {PrimaryInfoHash: pk1, InfoHashV2: v2a}}, Existing: []crawlerPersistTorrentsExistingV2{}},
		{Label: "v1_without_v2_unaffected", Items: []crawlerPersistTorrentsDedupItem{{PrimaryInfoHash: pk1, InfoHashV2: ""}, {PrimaryInfoHash: pk2, InfoHashV2: ""}}, Existing: []crawlerPersistTorrentsExistingV2{}},
	}
	results := make([]crawlerPersistTorrentsDedupCaseResult, 0, len(inputs))
	for _, input := range inputs {
		results = append(results, crawlerPersistTorrentsRunDedupCase(t, input))
	}
	return crawlerPersistTorrentsFixture{
		ID: crawlerPersistTorrentsFixtureIDs[4], Subsystem: "dht_crawler_persist_torrents",
		Classification: "RUNTIME_EXACT", Execution: "GO_ACTUAL_FILTER_V2_DUPLICATES_MATRIX",
		Oracle: crawlerPersistTorrentsOracle{
			Composition: "actual_filterV2Duplicates_with_fixed_in_memory_items_and_existing_map",
			Determinism: "ordered_slice_iteration_and_fixed_map_lookups_without_database_or_clock",
			Harness:     "none", Database: "not_executed", Clock: "not_read",
			RunPersistTorrentsExecuted: false, BatcherExecuted: false, DatabaseExecuted: false,
			ActualFunctionsExecuted: []string{
				"persist.filterV2Duplicates", "persist.dropV2Duplicate",
				"persist.filterV2Duplicates", "persist.dropV2Duplicate",
				"persist.filterV2Duplicates", "persist.dropV2Duplicate", "persist.dropV2Duplicate", "persist.dropV2Duplicate",
				"persist.filterV2Duplicates", "persist.dropV2Duplicate", "persist.dropV2Duplicate",
				"persist.filterV2Duplicates",
			},
			SourcePinnedHarnessSteps: []string{
				"construct_fixed_order_infoHashWithMetaInfo_slices",
				"construct_fixed_existing_full_v2_to_primary_maps",
				"project_kept_primary_hashes_in_returned_slice_order",
			},
		},
		Input:    crawlerPersistTorrentsInput{Kind: "filter_v2_duplicates_matrix", Cases: []crawlerPersistTorrentsModelCaseInput{}, DedupCases: inputs},
		Expected: crawlerPersistTorrentsExpected{Models: []crawlerPersistTorrentsModelResult{}, DedupCases: results, RunPersistTorrentsExecuted: false},
	}
}

func crawlerPersistTorrentsClassifierFixture(t *testing.T) crawlerPersistTorrentsFixture {
	t.Helper()
	input := crawlerPersistTorrentsClassifierInput{
		UniqueCount: 101, ClassifyBatchSize: classifyBatchSize,
		DuplicateInfoHash: crawlerPersistTorrentsOrdinalID(1).String(), FirstMarker: "first", LaterMarker: "later_duplicate",
	}
	result := crawlerPersistTorrentsRunClassifierHarness(t, input)
	return crawlerPersistTorrentsFixture{
		ID: crawlerPersistTorrentsFixtureIDs[5], Subsystem: "dht_crawler_persist_torrents",
		Classification: "RUNTIME_EXACT_TEST_HARNESS", Execution: "SOURCE_PINNED_PRIMARY_DEDUP_AND_CLASSIFIER_GROUPING_HARNESS_ONLY",
		Oracle: crawlerPersistTorrentsOracle{
			Composition: "source_pinned_exact_primary_hashMap_first_wins_and_flushHashesToClassify_grouping_harness",
			Determinism: "fixed_ordered_101_unique_hashes_one_exact_primary_duplicate_and_constant_batch_size",
			Harness:     "source_pinned_loop_harness_not_runPersistTorrents", Database: "not_executed", Clock: "read_but_absolute_run_after_excluded",
			RunPersistTorrentsExecuted: false, BatcherExecuted: false, DatabaseExecuted: false,
			ActualFunctionsExecuted: []string{
				"model.QueueJobDelayBy", "processor.NewQueueJob", "model.NewQueueJob",
				"model.QueueJobDelayBy", "processor.NewQueueJob", "model.NewQueueJob",
			},
			SourcePinnedHarnessSteps: []string{
				"iterate_fixed_input_in_order",
				"skip_later_exact_primary_key_occurrences",
				"append_each_first_unique_hash_to_classifier_slice",
				"flush_exactly_at_100_hashes",
				"flush_final_nonempty_suffix",
			},
		},
		Input:    crawlerPersistTorrentsInput{Kind: "primary_dedup_and_classifier_grouping_harness", Cases: []crawlerPersistTorrentsModelCaseInput{}, DedupCases: []crawlerPersistTorrentsDedupCaseInput{}, Classifier: &input},
		Expected: crawlerPersistTorrentsExpected{Models: []crawlerPersistTorrentsModelResult{}, DedupCases: []crawlerPersistTorrentsDedupCaseResult{}, Classifier: &result, RunPersistTorrentsExecuted: false},
	}
}

func crawlerPersistTorrentsRuntimeModelFixture(
	t *testing.T,
	id string,
	composition string,
	determinism string,
	inputs []crawlerPersistTorrentsModelCaseInput,
	summaryTime *time.Time,
) crawlerPersistTorrentsFixture {
	t.Helper()
	models := make([]crawlerPersistTorrentsModelResult, 0, len(inputs))
	for _, input := range inputs {
		models = append(models, crawlerPersistTorrentsExecuteModelCase(t, input, summaryTime))
	}
	execution := "GO_ACTUAL_PARSE_META_INFO_BYTES_AND_CREATE_TORRENT_MODEL"
	if id == crawlerPersistTorrentsFixtureIDs[2] {
		execution = "GO_ACTUAL_PARSE_MODEL_BLOB_DECODE_SUMMARY_AND_PIECES_MATRIX"
	} else if id == crawlerPersistTorrentsFixtureIDs[3] {
		execution = "GO_ACTUAL_PARSE_AND_MODEL_PURE_V2_AND_HYBRID_MATRIX"
	}
	return crawlerPersistTorrentsFixture{
		ID: id, Subsystem: "dht_crawler_persist_torrents", Classification: "RUNTIME_EXACT", Execution: execution,
		Oracle: crawlerPersistTorrentsOracle{
			Composition: composition, Determinism: determinism, Harness: "none", Database: "not_executed",
			Clock:                      map[bool]string{true: "fixed_test_clock_for_summary_only", false: "not_read"}[summaryTime != nil],
			RunPersistTorrentsExecuted: false, BatcherExecuted: false, DatabaseExecuted: false,
			ActualFunctionsExecuted:  crawlerPersistTorrentsRuntimeFunctions(id),
			SourcePinnedHarnessSteps: crawlerPersistTorrentsRuntimeHarnessSteps(id),
		},
		Input:    crawlerPersistTorrentsInput{Kind: "verified_raw_info_model_matrix", Cases: inputs, DedupCases: []crawlerPersistTorrentsDedupCaseInput{}},
		Expected: crawlerPersistTorrentsExpected{Models: models, DedupCases: []crawlerPersistTorrentsDedupCaseResult{}, RunPersistTorrentsExecuted: false},
	}
}

func crawlerPersistTorrentsRuntimeFunctions(id string) []string {
	switch id {
	case crawlerPersistTorrentsFixtureIDs[1]:
		return []string{"metainfo.ParseMetaInfoBytes", "persist.createTorrentModel"}
	case crawlerPersistTorrentsFixtureIDs[2]:
		perCase := []string{
			"metainfo.ParseMetaInfoBytes", "persist.createTorrentModel", "blob.SerializeFiles",
			"blob.ExtractUniqueExtensions", "blob.DeserializeFiles", "persist.buildTorrentFileSummary",
			"blob.BuildFileSummary", "blob.ExtractUniqueExtensions",
		}
		return append(append([]string{}, perCase...), perCase...)
	case crawlerPersistTorrentsFixtureIDs[3]:
		return []string{
			"metainfo.ParseMetaInfoBytes", "persist.createTorrentModel",
			"metainfo.ParseMetaInfoBytes", "persist.createTorrentModel", "blob.SerializeFiles",
			"blob.ExtractUniqueExtensions", "blob.DeserializeFiles",
		}
	default:
		panic("unexpected persist-torrents runtime model fixture ID: " + id)
	}
}

func crawlerPersistTorrentsRuntimeHarnessSteps(id string) []string {
	switch id {
	case crawlerPersistTorrentsFixtureIDs[1]:
		return []string{"construct_fixed_v1_single_Info", "bencode_Info", "derive_requested_v1_hash_from_raw_info"}
	case crawlerPersistTorrentsFixtureIDs[2]:
		return []string{
			"construct_fixed_exactly_N_and_N_plus_one_v1_Info_values",
			"bencode_each_Info_and_derive_requested_v1_hash",
			"supply_fixed_summary_clock_after_model_projection",
		}
	case crawlerPersistTorrentsFixtureIDs[3]:
		return []string{
			"encode_fixed_structurally_valid_BEP52_top_level_single_info_dictionary_with_synthetic_32_byte_pieces_root",
			"derive_requested_truncated_v2_hash_from_raw_info",
			"load_SHA_pinned_hybrid_torrent_and_extract_raw_info_dictionary",
			"derive_requested_v1_hash_from_hybrid_raw_info",
		}
	default:
		panic("unexpected persist-torrents runtime model fixture ID: " + id)
	}
}

func crawlerPersistTorrentsModelInput(
	t *testing.T,
	label string,
	info ami.Info,
	savePieces bool,
	threshold uint,
	sourceFixture string,
) crawlerPersistTorrentsModelCaseInput {
	t.Helper()
	raw, err := bencode.Marshal(info)
	if err != nil {
		t.Fatalf("marshal %s info: %v", label, err)
	}
	requested := crawlerPersistTorrentsV1Hash(raw)
	if info.MetaVersion == 2 && !info.HasV1() {
		requested = crawlerPersistTorrentsV2ShortHash(raw)
	}
	return crawlerPersistTorrentsRawModelInput(label, raw, requested, savePieces, threshold, sourceFixture)
}

func crawlerPersistTorrentsRawModelInput(
	label string,
	raw []byte,
	requested protocol.ID,
	savePieces bool,
	threshold uint,
	sourceFixture string,
) crawlerPersistTorrentsModelCaseInput {
	return crawlerPersistTorrentsModelCaseInput{
		Label: label, RawInfoHex: hex.EncodeToString(raw), RawInfoSHA256: fmt.Sprintf("%x", sha256.Sum256(raw)),
		RequestedInfoHash: requested.String(), SavePieces: savePieces, SaveFilesThreshold: threshold,
		SourceFixture: sourceFixture,
	}
}

func crawlerPersistTorrentsExecuteModelCase(
	t *testing.T,
	input crawlerPersistTorrentsModelCaseInput,
	summaryTime *time.Time,
) crawlerPersistTorrentsModelResult {
	t.Helper()
	raw, err := hex.DecodeString(input.RawInfoHex)
	if err != nil {
		t.Fatalf("decode %s raw info: %v", input.Label, err)
	}
	requested := protocol.MustParseID(input.RequestedInfoHash)
	parsed, parseErr := pmetainfo.ParseMetaInfoBytes(requested, raw)
	if parseErr != nil {
		t.Fatalf("parse %s verified raw info: %v", input.Label, parseErr)
	}
	if strconv.IntSize != 64 {
		t.Fatalf("persist-torrents oracle requires a 64-bit uint runtime, got %d bits", strconv.IntSize)
	}
	crawlerPersistTorrentsCheckedUint(t, parsed.Info.TotalLength(), input.Label+" total length")
	for index, file := range parsed.Info.UpvertedFiles() {
		crawlerPersistTorrentsCheckedUint(t, file.Length, fmt.Sprintf("%s file %d length", input.Label, index))
	}
	torrent, createErr := createTorrentModel(requested, parsed, input.SavePieces, input.SaveFilesThreshold)
	if createErr != nil {
		t.Fatalf("create %s torrent model: %v", input.Label, createErr)
	}
	result := crawlerPersistTorrentsProjectModel(t, input.Label, torrent, input.SavePieces)
	if summaryTime != nil && len(torrent.Files) > 0 {
		summary := buildTorrentFileSummary(requested, torrent.Files, torrent.FilesData, *summaryTime)
		result.Summary = &crawlerPersistTorrentsSummary{
			InfoHash: summary.InfoHash.String(), FileCount: summary.FileCount, TotalSize: summary.TotalSize,
			LargestFileSize: summary.LargestFileSize, Extensions: summary.Extensions,
			HasVideo: summary.HasVideo, HasSubtitle: summary.HasSubtitle, HasAudio: summary.HasAudio,
			CompressedBytesValid:            summary.CompressedBytes.Valid,
			CompressedBytes:                 summary.CompressedBytes.Int,
			CompressedBytesMatchesFilesData: summary.CompressedBytes.Valid && summary.CompressedBytes.Int == len(torrent.FilesData),
			CreatedAt:                       summary.CreatedAt.Format(time.RFC3339Nano), UpdatedAt: summary.UpdatedAt.Format(time.RFC3339Nano),
		}
	}
	return result
}

func crawlerPersistTorrentsProjectModel(
	t *testing.T,
	label string,
	torrent model.Torrent,
	savePieces bool,
) crawlerPersistTorrentsModelResult {
	t.Helper()
	files := crawlerPersistTorrentsProjectFiles(torrent.Files)
	var decoded []crawlerPersistTorrentsFile
	if len(torrent.FilesData) > 0 {
		decodedFiles, err := blobmigration.DeserializeFiles(torrent.FilesData)
		if err != nil {
			t.Fatalf("decode %s files_data: %v", label, err)
		}
		decoded = crawlerPersistTorrentsProjectFiles(decodedFiles)
	}
	var sources []crawlerPersistTorrentsSourceModel
	if torrent.Sources != nil {
		sources = make([]crawlerPersistTorrentsSourceModel, 0, len(torrent.Sources))
		for _, source := range torrent.Sources {
			sources = append(sources, crawlerPersistTorrentsSourceModel{Source: source.Source, InfoHash: source.InfoHash.String()})
		}
	}
	pieces := crawlerPersistTorrentsPieces{Present: savePieces, InfoHash: "", PieceLength: 0, PiecesHex: ""}
	if savePieces {
		pieces.InfoHash = torrent.Pieces.InfoHash.String()
		pieces.PieceLength = torrent.Pieces.PieceLength
		pieces.PiecesHex = hex.EncodeToString(torrent.Pieces.Pieces)
	}
	return crawlerPersistTorrentsModelResult{
		Label: label, ParseError: "", CreateError: "", InfoHash: torrent.InfoHash.String(),
		InfoHashV1:  crawlerPersistTorrentsOptionalID(torrent.InfoHashV1),
		InfoHashV2:  crawlerPersistTorrentsOptionalV2(torrent.InfoHashV2),
		MetaVersion: torrent.MetaVersion.Uint16, MetaVersionValid: torrent.MetaVersion.Valid,
		Name: torrent.Name, Size: torrent.Size, Private: torrent.Private, FilesStatus: torrent.FilesStatus.String(),
		FilesCount: torrent.FilesCount.Uint, FilesCountValid: torrent.FilesCount.Valid, Files: files,
		FilesNil: torrent.Files == nil, FilesDataPresent: len(torrent.FilesData) > 0, FilesDataNil: torrent.FilesData == nil,
		FilesDataByteLength: len(torrent.FilesData), FilesDataSHA256: crawlerPersistTorrentsOptionalBytesSHA256(torrent.FilesData),
		DecodedFiles: decoded, DecodedFilesNil: decoded == nil,
		DecodedFilesMatchRetainedCoreFields: crawlerPersistTorrentsDecodedCoreFieldsMatch(files, decoded),
		FileExtensions:                      torrent.FileExts, FileExtensionsNil: torrent.FileExts == nil,
		Sources: sources, SourcesNil: torrent.Sources == nil, Pieces: pieces,
	}
}

func crawlerPersistTorrentsProjectFiles(files []model.TorrentFile) []crawlerPersistTorrentsFile {
	if files == nil {
		return nil
	}
	result := make([]crawlerPersistTorrentsFile, 0, len(files))
	for _, file := range files {
		result = append(result, crawlerPersistTorrentsFile{
			Index: file.Index, Path: file.Path, Size: file.Size,
			Extension: file.Extension.String, ExtensionValid: file.Extension.Valid,
		})
	}
	return result
}

func crawlerPersistTorrentsCheckedUint(t *testing.T, value int64, label string) uint {
	t.Helper()
	if value < 0 || uint64(value) > uint64(^uint(0)) {
		t.Fatalf("%s = %d does not fit uint", label, value)
	}
	return uint(value)
}

func crawlerPersistTorrentsOptionalBytesSHA256(value []byte) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256(value))
}

func crawlerPersistTorrentsDecodedCoreFieldsMatch(
	retained []crawlerPersistTorrentsFile,
	decoded []crawlerPersistTorrentsFile,
) bool {
	if len(retained) == 0 {
		return len(decoded) == 0
	}
	if len(retained) != len(decoded) {
		return false
	}
	for index := range retained {
		if retained[index].Index != decoded[index].Index || retained[index].Path != decoded[index].Path || retained[index].Size != decoded[index].Size {
			return false
		}
	}
	return true
}

func crawlerPersistTorrentsRunDedupCase(
	t *testing.T,
	input crawlerPersistTorrentsDedupCaseInput,
) crawlerPersistTorrentsDedupCaseResult {
	t.Helper()
	items := make([]infoHashWithMetaInfo, 0, len(input.Items))
	for _, item := range input.Items {
		var fullV2 *protocol.InfoHashV2
		if item.InfoHashV2 != "" {
			parsed := crawlerPersistTorrentsParseV2(t, item.InfoHashV2)
			fullV2 = &parsed
		}
		items = append(items, infoHashWithMetaInfo{
			nodeHasPeersForHash: nodeHasPeersForHash{infoHash: protocol.MustParseID(item.PrimaryInfoHash)},
			metaInfo:            pmetainfo.ParsedInfo{InfoHashV2: fullV2},
		})
	}
	existing := make(map[protocol.InfoHashV2]protocol.ID, len(input.Existing))
	for _, item := range input.Existing {
		existing[crawlerPersistTorrentsParseV2(t, item.InfoHashV2)] = protocol.MustParseID(item.PrimaryInfoHash)
	}
	kept, dropped := filterV2Duplicates(items, existing)
	keptHashes := make([]string, 0, len(kept))
	for _, item := range kept {
		keptHashes = append(keptHashes, item.infoHash.String())
	}
	return crawlerPersistTorrentsDedupCaseResult{Label: input.Label, KeptPrimaryInfoHashes: keptHashes, Dropped: dropped}
}

func crawlerPersistTorrentsRunClassifierHarness(
	t *testing.T,
	input crawlerPersistTorrentsClassifierInput,
) crawlerPersistTorrentsClassifierResult {
	t.Helper()
	if input.UniqueCount != 101 || input.ClassifyBatchSize != classifyBatchSize {
		t.Fatalf("classifier harness bounds = %d/%d, want 101/%d", input.UniqueCount, input.ClassifyBatchSize, classifyBatchSize)
	}
	type harnessItem struct {
		hash   protocol.ID
		marker string
	}
	items := make([]harnessItem, 0, input.UniqueCount+1)
	for ordinal := 1; ordinal <= input.UniqueCount; ordinal++ {
		marker := fmt.Sprintf("unique_%03d", ordinal)
		if ordinal == 1 {
			marker = input.FirstMarker
		}
		items = append(items, harnessItem{hash: crawlerPersistTorrentsOrdinalID(ordinal), marker: marker})
		if ordinal == 1 {
			items = append(items, harnessItem{hash: crawlerPersistTorrentsOrdinalID(ordinal), marker: input.LaterMarker})
		}
	}
	seen := make(map[protocol.ID]harnessItem, input.UniqueCount)
	duplicates := []string{}
	groups := make([][]string, 0, 2)
	queueJobs := make([]crawlerPersistTorrentsQueueJob, 0, 2)
	pending := make([]protocol.ID, 0, input.ClassifyBatchSize)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		group := make([]string, 0, len(pending))
		for _, hash := range pending {
			group = append(group, hash.String())
		}
		groups = append(groups, group)
		job, err := processor.NewQueueJob(
			processor.MessageParams{InfoHashes: append([]protocol.ID(nil), pending...)},
			model.QueueJobDelayBy(time.Minute),
		)
		if err != nil {
			t.Fatalf("create classifier queue job: %v", err)
		}
		queueJobs = append(queueJobs, crawlerPersistTorrentsQueueJob{
			Queue: job.Queue, Payload: job.Payload, Fingerprint: job.Fingerprint, Status: string(job.Status),
			Retries: job.Retries, MaxRetries: job.MaxRetries, Priority: job.Priority,
			ArchivalDurationNanoseconds: int64(job.ArchivalDuration), DelayMillis: time.Minute.Milliseconds(),
			AbsoluteRunAfterExcluded: true,
		})
		pending = make([]protocol.ID, 0, input.ClassifyBatchSize)
	}
	for _, item := range items {
		if _, ok := seen[item.hash]; ok {
			duplicates = append(duplicates, item.hash.String())
			continue
		}
		seen[item.hash] = item
		pending = append(pending, item.hash)
		if len(pending) >= input.ClassifyBatchSize {
			flush()
		}
	}
	flush()
	winner := seen[protocol.MustParseID(input.DuplicateInfoHash)].marker
	return crawlerPersistTorrentsClassifierResult{
		InputCount: len(items), UniqueCount: len(seen), DuplicateInfoHashes: duplicates,
		DuplicateWinnerMarker: winner, ClassifierGroups: groups, QueueJobs: queueJobs,
	}
}

func crawlerPersistTorrentsLoadInfoBytes(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(crawlerPersistTorrentsRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	meta, err := ami.Load(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), meta.InfoBytes...)
}

func crawlerPersistTorrentsV1Hash(raw []byte) protocol.ID {
	return protocol.ID(ami.HashBytes(raw))
}

func crawlerPersistTorrentsV2ShortHash(raw []byte) (id protocol.ID) {
	full := sha256.Sum256(raw)
	copy(id[:], full[:20])
	return id
}

func crawlerPersistTorrentsOptionalID(value *protocol.ID) string {
	if value == nil {
		return ""
	}
	return value.String()
}

func crawlerPersistTorrentsOptionalV2(value *protocol.InfoHashV2) string {
	if value == nil {
		return ""
	}
	return hex.EncodeToString(value[:])
}

func crawlerPersistTorrentsIDHex(value byte) string {
	var id protocol.ID
	for index := range id {
		id[index] = value
	}
	return id.String()
}

func crawlerPersistTorrentsV2Hex(value byte) string {
	var id protocol.InfoHashV2
	for index := range id {
		id[index] = value
	}
	return hex.EncodeToString(id[:])
}

func crawlerPersistTorrentsOrdinalID(ordinal int) (id protocol.ID) {
	if ordinal <= 0 || ordinal > 0xffff {
		panic("persist-torrents ordinal is outside uint16")
	}
	id[18] = byte(ordinal >> 8)
	id[19] = byte(ordinal)
	return id
}

func crawlerPersistTorrentsParseV2(t *testing.T, value string) (id protocol.InfoHashV2) {
	t.Helper()
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != len(id) {
		t.Fatalf("invalid full v2 hash %q", value)
	}
	copy(id[:], raw)
	return id
}

func crawlerPersistTorrentsDependencyLines(t *testing.T) []string {
	t.Helper()
	return []string{
		crawlerPersistTorrentsDependencyLine(t, "go.mod", "github.com/anacrolix/torrent "),
		crawlerPersistTorrentsDependencyLine(t, "go.mod", "github.com/klauspost/compress "),
		crawlerPersistTorrentsDependencyLine(t, "go.mod", "github.com/vmihailenco/msgpack/v5 "),
		crawlerPersistTorrentsDependencyLine(t, "go.mod", "gorm.io/gen "),
		crawlerPersistTorrentsDependencyLine(t, "go.mod", "gorm.io/gorm "),
	}
}

func crawlerPersistTorrentsNormalizedASTDigests(t *testing.T) map[string]string {
	t.Helper()
	specs := []crawlerPersistTorrentsASTSpec{
		{key: "batching.NewBatchingChannel", path: "internal/concurrency/batching_channel.go", kind: "func", name: "NewBatchingChannel"},
		{key: "batching.In", path: "internal/concurrency/batching_channel.go", kind: "func", name: "In", receiver: "*batchingChannel[T]"},
		{key: "batching.Out", path: "internal/concurrency/batching_channel.go", kind: "func", name: "Out", receiver: "*batchingChannel[T]"},
		{key: "batching.batch", path: "internal/concurrency/batching_channel.go", kind: "func", name: "batch", receiver: "*batchingChannel[T]"},
		{key: "batching.flush", path: "internal/concurrency/batching_channel.go", kind: "func", name: "flush", receiver: "*batchingChannel[T]"},
		{key: "crawler.infoHashWithMetaInfo", path: "internal/dhtcrawler/crawler.go", kind: "type", name: "infoHashWithMetaInfo"},
		{key: "crawler.start", path: "internal/dhtcrawler/crawler.go", kind: "func", name: "start", receiver: "*crawler"},
		{key: "config.Config", path: "internal/dhtcrawler/config.go", kind: "type", name: "Config"},
		{key: "config.NewDefaultConfig", path: "internal/dhtcrawler/config.go", kind: "func", name: "NewDefaultConfig"},
		{key: "factory.Params", path: "internal/dhtcrawler/factory.go", kind: "type", name: "Params"},
		{key: "factory.Result", path: "internal/dhtcrawler/factory.go", kind: "type", name: "Result"},
		{key: "factory.New", path: "internal/dhtcrawler/factory.go", kind: "func", name: "New"},
		{key: "persist.runPersistTorrents", path: "internal/dhtcrawler/persist.go", kind: "func", name: "runPersistTorrents", receiver: "*crawler"},
		{key: "persist.createTorrentModel", path: "internal/dhtcrawler/persist.go", kind: "func", name: "createTorrentModel"},
		{key: "persist.buildTorrentFileSummary", path: "internal/dhtcrawler/persist.go", kind: "func", name: "buildTorrentFileSummary"},
		{key: "persist.dropV2Duplicate", path: "internal/dhtcrawler/persist.go", kind: "func", name: "dropV2Duplicate"},
		{key: "persist.filterV2Duplicates", path: "internal/dhtcrawler/persist.go", kind: "func", name: "filterV2Duplicates"},
		{key: "persist.lookupExistingV2", path: "internal/dhtcrawler/persist.go", kind: "func", name: "lookupExistingV2", receiver: "*crawler"},
		{key: "persist.torrentFileSummaryPersistQuery", path: "internal/dhtcrawler/persist.go", kind: "func", name: "torrentFileSummaryPersistQuery"},
		{key: "metainfo.Info", path: "internal/protocol/metainfo/metainfo.go", kind: "type", name: "Info"},
		{key: "metainfo.ParsedInfo", path: "internal/protocol/metainfo/parse.go", kind: "type", name: "ParsedInfo"},
		{key: "metainfo.ParseMetaInfoBytes", path: "internal/protocol/metainfo/parse.go", kind: "func", name: "ParseMetaInfoBytes"},
		{key: "blob.SerializeFiles", path: "internal/blobmigration/serializer.go", kind: "func", name: "SerializeFiles"},
		{key: "blob.DeserializeFiles", path: "internal/blobmigration/serializer.go", kind: "func", name: "DeserializeFiles"},
		{key: "blob.ExtractUniqueExtensions", path: "internal/blobmigration/serializer.go", kind: "func", name: "ExtractUniqueExtensions"},
		{key: "blob.BuildFileSummary", path: "internal/blobmigration/serializer.go", kind: "func", name: "BuildFileSummary"},
		{key: "model.Torrent", path: "internal/model/torrents.gen.go", kind: "type", name: "Torrent"},
		{key: "model.TorrentFile", path: "internal/model/torrent_files.gen.go", kind: "type", name: "TorrentFile"},
		{key: "model.TorrentFileSummary", path: "internal/model/torrent_file_summary.go", kind: "type", name: "TorrentFileSummary"},
		{key: "model.TorrentsTorrentSource", path: "internal/model/torrents_torrent_sources.gen.go", kind: "type", name: "TorrentsTorrentSource"},
		{key: "model.TorrentPieces", path: "internal/model/torrent_pieces.gen.go", kind: "type", name: "TorrentPieces"},
		{key: "model.QueueJob", path: "internal/model/queue_jobs.gen.go", kind: "type", name: "QueueJob"},
		{key: "model.NewQueueJob", path: "internal/model/queue_jobs.go", kind: "func", name: "NewQueueJob"},
		{key: "model.QueueJobDelayBy", path: "internal/model/queue_jobs.go", kind: "func", name: "QueueJobDelayBy"},
		{key: "processor.MessageParams", path: "internal/processor/message.go", kind: "type", name: "MessageParams"},
		{key: "processor.NewQueueJob", path: "internal/processor/message.go", kind: "func", name: "NewQueueJob"},
		{key: "protocol.InfoHashV2.ToShort", path: "internal/protocol/infohash_v2.go", kind: "func", name: "ToShort", receiver: "InfoHashV2"},
	}
	digests := make(map[string]string, len(specs))
	for _, specification := range specs {
		node, files := crawlerPersistTorrentsFindASTNode(t, specification)
		var normalized bytes.Buffer
		if err := format.Node(&normalized, files, node); err != nil {
			t.Fatal(err)
		}
		digests[specification.key] = fmt.Sprintf("%x", sha256.Sum256(normalized.Bytes()))
	}
	if len(crawlerPersistTorrentsExpectedNormalizedASTSHA256) != 0 && !*updateDHTCrawlerPersistTorrentsOracle {
		if len(digests) != len(crawlerPersistTorrentsExpectedNormalizedASTSHA256) {
			t.Fatalf("normalized AST digest count = %d, want %d", len(digests), len(crawlerPersistTorrentsExpectedNormalizedASTSHA256))
		}
		for key, actual := range digests {
			if expected := crawlerPersistTorrentsExpectedNormalizedASTSHA256[key]; actual != expected {
				t.Fatalf("normalized AST SHA-256 %s = %s, want %s", key, actual, expected)
			}
		}
	} else if !*updateDHTCrawlerPersistTorrentsOracle {
		encoded, marshalErr := json.MarshalIndent(digests, "", "  ")
		if marshalErr != nil {
			t.Fatalf("marshal normalized AST digests: %v", marshalErr)
		}
		t.Fatalf("fill crawlerPersistTorrentsExpectedNormalizedASTSHA256 with:\n%s", encoded)
	}
	return digests
}

func crawlerPersistTorrentsFindASTNode(
	t *testing.T,
	specification crawlerPersistTorrentsASTSpec,
) (ast.Node, *token.FileSet) {
	t.Helper()
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, filepath.Join(crawlerPersistTorrentsRoot(t), specification.path), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var matches []ast.Node
	for _, declaration := range file.Decls {
		switch typed := declaration.(type) {
		case *ast.FuncDecl:
			if specification.kind == "func" && typed.Name.Name == specification.name && crawlerPersistTorrentsReceiver(files, typed) == specification.receiver {
				matches = append(matches, typed)
			}
		case *ast.GenDecl:
			if specification.kind != "type" {
				continue
			}
			for _, raw := range typed.Specs {
				if typeSpec, ok := raw.(*ast.TypeSpec); ok && typeSpec.Name.Name == specification.name {
					matches = append(matches, typeSpec)
				}
			}
		}
	}
	if len(matches) != 1 {
		t.Fatalf("%s %s receiver %q matches in %s = %d, want exactly 1", specification.kind, specification.name, specification.receiver, specification.path, len(matches))
	}
	return matches[0], files
}

func crawlerPersistTorrentsReceiver(files *token.FileSet, declaration *ast.FuncDecl) string {
	if declaration.Recv == nil || len(declaration.Recv.List) == 0 {
		return ""
	}
	var formatted bytes.Buffer
	if err := format.Node(&formatted, files, declaration.Recv.List[0].Type); err != nil {
		panic(err)
	}
	return formatted.String()
}

func crawlerPersistTorrentsSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	paths := []string{
		"go.mod", "go.sum", "internal/blobmigration/serializer.go", "internal/concurrency/batching_channel.go",
		"internal/dhtcrawler/config.go", "internal/dhtcrawler/crawler.go", "internal/dhtcrawler/factory.go", "internal/dhtcrawler/persist.go",
		"internal/dhtcrawler/request_meta_info.go", "internal/model/files_status.go", "internal/model/files_status_enum.go",
		"internal/model/duration.go", "internal/model/file_type.go", "internal/model/file_type_enum.go", "internal/model/null.go",
		"internal/model/queue_job_status.go", "internal/model/queue_job_status_enum.go", "internal/model/queue_jobs.go", "internal/model/queue_jobs.gen.go",
		"internal/model/torrent_file_summary.go", "internal/model/torrent_files.gen.go", "internal/model/torrent_pieces.gen.go",
		"internal/model/torrent_files.go",
		"internal/model/torrents.gen.go", "internal/model/torrents_torrent_sources.gen.go", "internal/processor/message.go",
		"internal/protocol/id.go", "internal/protocol/metainfo/metainfo.go", "internal/protocol/metainfo/parse.go",
		"internal/protocol/infohash_v2.go",
		"migrations/00001_init.sql", "migrations/00002_files_status.sql", "migrations/00012_queue.sql", "migrations/00013_torrent_pieces.sql",
		"migrations/00015_queue_priority.sql", "migrations/00019_queue_fix_duplicate_key.sql",
		"migrations/00016_files.sql", "migrations/00017_ordering_fields.sql", "migrations/00021_blob_storage.sql",
		"migrations/00023_v2_infohash.sql", "migrations/00025_dht_seen_count.sql", "migrations/00026_summary_compressed_bytes.sql",
	}
	digests := make(map[string]string, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(filepath.Join(crawlerPersistTorrentsRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		digests[path] = fmt.Sprintf("%x", sha256.Sum256(contents))
	}
	return digests
}

func crawlerPersistTorrentsPrerequisiteDigests(t *testing.T) map[string]string {
	t.Helper()
	want := map[string]string{
		"testdata/parity/dht/dht_crawler_request_meta_info.jsonl":        "03ce2ab0da2b0f9ba1173b8ba52481a903265ca6862f957b40490cf67a9e4ec5",
		"internal/dhtcrawler/testdata/bittorrent-v2-hybrid-test.torrent": "8ba7575e64e9046cac74ca6523bff6445ff5c3e369d5d132607a793a1834e93f",
		"testdata/parity/queue/fingerprints.jsonl":                       "5636896337cf3c27cda78eae4d4315f48bc4c447300beecfef55b35a5f831a8b",
	}
	for path, expected := range want {
		contents, err := os.ReadFile(filepath.Join(crawlerPersistTorrentsRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		if actual := fmt.Sprintf("%x", sha256.Sum256(contents)); actual != expected {
			t.Fatalf("%s SHA-256 = %s, want %s", path, actual, expected)
		}
	}
	return want
}

func crawlerPersistTorrentsDependencyLine(t *testing.T, path string, prefix string) string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(crawlerPersistTorrentsRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(contents), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, prefix) {
			return trimmed
		}
	}
	t.Fatalf("dependency line with prefix %q not found in %s", prefix, path)
	return ""
}

func crawlerPersistTorrentsRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve persist-torrents oracle source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
}

func crawlerPersistTorrentsReconcile(t *testing.T, fixtures []crawlerPersistTorrentsFixture) {
	t.Helper()
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	for _, fixture := range fixtures {
		if err := encoder.Encode(fixture); err != nil {
			t.Fatal(err)
		}
	}
	if encoded.Len() == 0 || encoded.Bytes()[encoded.Len()-1] != '\n' || bytes.Contains(encoded.Bytes(), []byte("\r")) {
		t.Fatal("persist-torrents fixture must be nonempty LF-only JSONL with a final LF")
	}
	crawlerPersistTorrentsValidateStrictJSONL(t, encoded.Bytes(), fixtures)
	actualHash := fmt.Sprintf("%x", sha256.Sum256(encoded.Bytes()))
	if crawlerPersistTorrentsFixtureSHA256 != "" && actualHash != crawlerPersistTorrentsFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerPersistTorrentsFixtureSHA256)
	}
	path := filepath.Join(crawlerPersistTorrentsRoot(t), "testdata/parity/dht/dht_crawler_persist_torrents.jsonl")
	if *updateDHTCrawlerPersistTorrentsOracle {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-persist-torrents-oracle: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler persist-torrents fixture is stale; rerun with -update-dht-crawler-persist-torrents-oracle")
	}
}

func crawlerPersistTorrentsValidateStrictJSONL(
	t *testing.T,
	contents []byte,
	want []crawlerPersistTorrentsFixture,
) {
	t.Helper()
	if bytes.Count(contents, []byte("\n")) != len(want) {
		t.Fatalf("fixture LF count = %d, want %d", bytes.Count(contents, []byte("\n")), len(want))
	}
	scanner := bufio.NewScanner(bytes.NewReader(contents))
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 16*1024*1024)
	decoded := make([]crawlerPersistTorrentsFixture, 0, len(want))
	for scanner.Scan() {
		decoder := json.NewDecoder(strings.NewReader(scanner.Text()))
		decoder.DisallowUnknownFields()
		var fixture crawlerPersistTorrentsFixture
		if err := decoder.Decode(&fixture); err != nil {
			t.Fatalf("strict decode row %d: %v", len(decoded)+1, err)
		}
		var extra json.RawMessage
		if err := decoder.Decode(&extra); err != io.EOF {
			t.Fatalf("strict decode row %d trailing JSON: %v", len(decoded)+1, err)
		}
		crawlerPersistTorrentsValidateWidths(t, fixture)
		decoded = append(decoded, fixture)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(want) {
		t.Fatalf("strict decoded row count = %d, want %d", len(decoded), len(want))
	}
	for index := range want {
		if decoded[index].ID != want[index].ID || decoded[index].Classification != want[index].Classification || decoded[index].Execution != want[index].Execution {
			t.Fatalf("strict decoded row %d identity/classification/execution drift", index+1)
		}
	}
}

func crawlerPersistTorrentsValidateWidths(t *testing.T, fixture crawlerPersistTorrentsFixture) {
	t.Helper()
	for _, input := range fixture.Input.Cases {
		crawlerPersistTorrentsRequireHex(t, input.RequestedInfoHash, 20, input.Label+" requested info hash")
		crawlerPersistTorrentsRequireHex(t, input.RawInfoSHA256, 32, input.Label+" raw info SHA-256")
		crawlerPersistTorrentsRequireVariableHex(t, input.RawInfoHex, input.Label+" raw info")
	}
	for _, result := range fixture.Expected.Models {
		if result.InfoHash != "" {
			crawlerPersistTorrentsRequireHex(t, result.InfoHash, 20, result.Label+" model info hash")
		}
		if result.InfoHashV1 != "" {
			crawlerPersistTorrentsRequireHex(t, result.InfoHashV1, 20, result.Label+" model v1 hash")
		}
		if result.InfoHashV2 != "" {
			crawlerPersistTorrentsRequireHex(t, result.InfoHashV2, 32, result.Label+" model v2 hash")
		}
		if result.FilesDataPresent {
			crawlerPersistTorrentsRequireHex(t, result.FilesDataSHA256, 32, result.Label+" files_data SHA-256")
		} else if result.FilesDataSHA256 != "" || result.FilesDataByteLength != 0 {
			t.Fatalf("%s absent files_data has digest/length", result.Label)
		}
		for _, source := range result.Sources {
			crawlerPersistTorrentsRequireHex(t, source.InfoHash, 20, result.Label+" source info hash")
		}
		if result.Pieces.Present {
			crawlerPersistTorrentsRequireHex(t, result.Pieces.InfoHash, 20, result.Label+" pieces info hash")
			crawlerPersistTorrentsRequireVariableHex(t, result.Pieces.PiecesHex, result.Label+" pieces")
		}
	}
	for _, input := range fixture.Input.DedupCases {
		for _, item := range input.Items {
			crawlerPersistTorrentsRequireHex(t, item.PrimaryInfoHash, 20, input.Label+" primary info hash")
			if item.InfoHashV2 != "" {
				crawlerPersistTorrentsRequireHex(t, item.InfoHashV2, 32, input.Label+" v2 hash")
			}
		}
		for _, item := range input.Existing {
			crawlerPersistTorrentsRequireHex(t, item.PrimaryInfoHash, 20, input.Label+" existing primary info hash")
			crawlerPersistTorrentsRequireHex(t, item.InfoHashV2, 32, input.Label+" existing v2 hash")
		}
	}
	if fixture.Input.Classifier != nil {
		crawlerPersistTorrentsRequireHex(t, fixture.Input.Classifier.DuplicateInfoHash, 20, "classifier duplicate info hash")
		if fixture.Expected.Classifier == nil {
			t.Fatal("classifier input is missing classifier result")
		}
		for _, hash := range fixture.Expected.Classifier.DuplicateInfoHashes {
			crawlerPersistTorrentsRequireHex(t, hash, 20, "classifier duplicate hash")
		}
		for _, group := range fixture.Expected.Classifier.ClassifierGroups {
			for _, hash := range group {
				crawlerPersistTorrentsRequireHex(t, hash, 20, "classifier group hash")
			}
		}
		for _, job := range fixture.Expected.Classifier.QueueJobs {
			crawlerPersistTorrentsRequireHex(t, job.Fingerprint, 32, "classifier queue job fingerprint")
		}
	}
	if fixture.Expected.Source != nil {
		for path, digest := range fixture.Expected.Source.SourceSHA256 {
			crawlerPersistTorrentsRequireHex(t, digest, 32, path+" source SHA-256")
		}
		for path, digest := range fixture.Expected.Source.PrerequisiteSHA256 {
			crawlerPersistTorrentsRequireHex(t, digest, 32, path+" prerequisite SHA-256")
		}
		for key, digest := range fixture.Expected.Source.NormalizedASTSHA256 {
			crawlerPersistTorrentsRequireHex(t, digest, 32, key+" normalized AST SHA-256")
		}
	}
}

func crawlerPersistTorrentsRequireHex(t *testing.T, value string, width int, label string) {
	t.Helper()
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != width || value != strings.ToLower(value) {
		t.Fatalf("%s = %q, want lowercase %d-byte hex", label, value, width)
	}
}

func crawlerPersistTorrentsRequireVariableHex(t *testing.T, value string, label string) {
	t.Helper()
	if value == "" || value != strings.ToLower(value) {
		t.Fatalf("%s must be nonempty lowercase hex", label)
	}
	if _, err := hex.DecodeString(value); err != nil {
		t.Fatalf("%s is not hex: %v", label, err)
	}
}
