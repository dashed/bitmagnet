package dhtcrawler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

var updateDHTCrawlerInfoHashTriageParity = flag.Bool(
	"update-dht-crawler-info-hash-triage-parity",
	false,
	"rewrite the Rust DHT crawler info-hash-triage parity fixture",
)

const crawlerInfoHashTriageFixtureSHA256 = "52eda840f872225cc34f8cf12edc2e4621e8a1fef569abf34a50f4a3bd9896f8"

var crawlerInfoHashTriageExpectedNormalizedASTSHA256 = map[string]string{
	"config.NewDefaultConfig":     "d044a4710817daf9a87dfab03ce22f138da3c6e1bf94d40bbbfd0fea70673f32",
	"crawler.nodeHasPeersForHash": "1e2206b038dd5c1b70dff5a29cdf044ad7133b4876db75723081ab37c3d3da58",
	"crawler.start":               "d61a318ce626352ee4f5cd5dd48191d767bbfe45b6a9def673cd185eada4f67b",
	"factory.New":                 "0204a00fd63b275339d63d622865858571c153bc81fc738784a78e1c150fec80",
	"infohash.triageResult":       "fc0569527db2ab92684d7b4585f6971a95bb16e8698a7f80f2d68bbacb9e1435",
	"infohash.runInfoHashTriage":  "1009e7775daf5ee49c53f7655130e98ec6a8f1e9574fb0f0b044ff3156f54b96",
}

var crawlerInfoHashTriageFixtureIDs = [...]string{
	"production_source_factory_and_lifecycle_contract",
	"dedup_filter_lookup_and_decision_matrix",
	"empty_filter_result_skips_database_and_outputs",
	"filter_error_drops_batch_and_continues",
	"database_error_drops_batch_and_continues",
	"cancellation_at_blocked_get_peers_send",
	"cancellation_at_blocked_scrape_send",
}

type crawlerInfoHashTriageFixture struct {
	ID             string                        `json:"id"`
	Subsystem      string                        `json:"subsystem"`
	Classification string                        `json:"classification"`
	Oracle         crawlerInfoHashTriageOracle   `json:"oracle"`
	Input          crawlerInfoHashTriageInput    `json:"input"`
	Expected       crawlerInfoHashTriageExpected `json:"expected"`
}

type crawlerInfoHashTriageOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Database    string `json:"database"`
	Clock       string `json:"clock"`
}

type crawlerInfoHashTriageInput struct {
	Kind            string                             `json:"kind"`
	Batches         [][]crawlerInfoHashTriageRequest   `json:"batches"`
	FilterSteps     []crawlerInfoHashTriageFilterStep  `json:"filterSteps"`
	DatabaseRows    []crawlerInfoHashTriageDatabaseRow `json:"databaseRows"`
	DatabaseError   string                             `json:"databaseError"`
	CancelAtLane    string                             `json:"cancelAtLane"`
	SaveFiles       uint                               `json:"saveFilesThreshold"`
	RescrapeSeconds int64                              `json:"rescrapeThresholdSeconds"`
}

type crawlerInfoHashTriageRequest struct {
	InfoHash string `json:"infoHash"`
	Node     string `json:"node"`
}

type crawlerInfoHashTriageFilterStep struct {
	Result []string `json:"result"`
	Error  string   `json:"error"`
}

type crawlerInfoHashTriageDatabaseRow struct {
	InfoHash    string `json:"infoHash"`
	FilesStatus string `json:"filesStatus"`
	FilesCount  *uint  `json:"filesCount"`
	Seeders     *uint  `json:"seeders"`
	Leechers    *uint  `json:"leechers"`
	UpdatedAt   string `json:"updatedAt"`
}

type crawlerInfoHashTriageRuntimeDatabaseRow struct {
	InfoHash    protocol.ID
	FilesStatus model.FilesStatus
	FilesCount  *uint
	Seeders     *uint
	Leechers    *uint
	UpdatedAt   time.Time
}

type crawlerInfoHashTriageExpected struct {
	FilterCalls        [][]string                    `json:"filterCalls"`
	SQLArgs            []string                      `json:"sqlArgs"`
	DatabaseQueryCalls int                           `json:"databaseQueryCalls"`
	Actions            []crawlerInfoHashTriageAction `json:"actions"`
	GetPeersInCalls    int                           `json:"getPeersInCalls"`
	ScrapeInCalls      int                           `json:"scrapeInCalls"`
	BlockCalls         int                           `json:"blockCalls"`
	FlushCalls         int                           `json:"flushCalls"`
	RunReturned        bool                          `json:"runReturned"`
	ContextCancelled   bool                          `json:"contextCancelled"`
	ContinuedAfterErr  bool                          `json:"continuedAfterError"`
	Source             *crawlerInfoHashTriageSource  `json:"source,omitempty"`
}

type crawlerInfoHashTriageAction struct {
	Action   string `json:"action"`
	InfoHash string `json:"infoHash"`
	Node     string `json:"node"`
}

type crawlerInfoHashTriageSource struct {
	InputCapacity                       int               `json:"inputCapacity"`
	BatchLimit                          int               `json:"batchLimit"`
	BatchIntervalSeconds                int               `json:"batchIntervalSeconds"`
	BatchOutputCapacity                 int               `json:"batchOutputCapacity"`
	GetPeersInputCapacity               int               `json:"getPeersInputCapacity"`
	GetPeersConcurrency                 int               `json:"getPeersConcurrency"`
	ScrapeInputCapacity                 int               `json:"scrapeInputCapacity"`
	ScrapeConcurrency                   int               `json:"scrapeConcurrency"`
	DefaultScalingFactor                int               `json:"defaultScalingFactor"`
	DefaultSaveFilesThreshold           uint              `json:"defaultSaveFilesThreshold"`
	DefaultRescrapeThresholdSeconds     int64             `json:"defaultRescrapeThresholdSeconds"`
	FirstDuplicateWins                  bool              `json:"firstDuplicateWins"`
	FilterReceivesFirstUniqueOrder      bool              `json:"filterReceivesFirstUniqueOrder"`
	FilterBeforeDatabase                bool              `json:"filterBeforeDatabase"`
	FilteredHashesDedupedForRouting     bool              `json:"filteredHashesDedupedForRouting"`
	FilteredDuplicatesRemainSQLArgs     bool              `json:"filteredDuplicatesRemainSqlArgs"`
	DatabaseDuplicateLastWins           bool              `json:"databaseDuplicateLastWins"`
	SelectedColumns                     []string          `json:"selectedColumns"`
	JoinKind                            string            `json:"joinKind"`
	JoinSource                          string            `json:"joinSource"`
	GetPeersPrecedesScrape              bool              `json:"getPeersPrecedesScrape"`
	StrictStaleBefore                   bool              `json:"strictStaleBefore"`
	TimeNowReadPerReachedStalenessCheck bool              `json:"timeNowReadPerReachedStalenessCheck"`
	ErrorBreakContinuesOuterLoop        bool              `json:"errorBreakContinuesOuterLoop"`
	SendsCancellationAware              bool              `json:"sendsCancellationAware"`
	ClosedOutChecksOpenBoolean          bool              `json:"closedOutChecksOpenBoolean"`
	WorkerDetached                      bool              `json:"workerDetached"`
	WorkerJoined                        bool              `json:"workerJoined"`
	NoStats                             bool              `json:"noStats"`
	NormalizedASTSHA256                 map[string]string `json:"normalizedAstSha256"`
	SourceSHA256                        map[string]string `json:"sourceSha256"`
	GoModSQLMockLine                    string            `json:"goModSqlmockLine"`
	GoSumSQLMockLine                    string            `json:"goSumSqlmockLine"`
	Evidence                            string            `json:"evidence"`
	Nonclaims                           []string          `json:"nonclaims"`
}

type crawlerInfoHashTriageBatchLane struct {
	input  chan nodeHasPeersForHash
	output chan []nodeHasPeersForHash
}

func newCrawlerInfoHashTriageBatchLane() *crawlerInfoHashTriageBatchLane {
	return &crawlerInfoHashTriageBatchLane{
		input: make(chan nodeHasPeersForHash), output: make(chan []nodeHasPeersForHash, 8),
	}
}

func (l *crawlerInfoHashTriageBatchLane) In() chan<- nodeHasPeersForHash    { return l.input }
func (l *crawlerInfoHashTriageBatchLane) Out() <-chan []nodeHasPeersForHash { return l.output }

type crawlerInfoHashTriageDeliveryLane struct {
	input    chan nodeHasPeersForHash
	accessed chan int
	mutex    sync.Mutex
	calls    int
}

func newCrawlerInfoHashTriageDeliveryLane(capacity int) *crawlerInfoHashTriageDeliveryLane {
	return &crawlerInfoHashTriageDeliveryLane{
		input: make(chan nodeHasPeersForHash, capacity), accessed: make(chan int, 16),
	}
}

func (l *crawlerInfoHashTriageDeliveryLane) In() chan<- nodeHasPeersForHash {
	l.mutex.Lock()
	l.calls++
	call := l.calls
	l.mutex.Unlock()
	l.accessed <- call
	return l.input
}

func (*crawlerInfoHashTriageDeliveryLane) Run(context.Context, func(nodeHasPeersForHash)) error {
	panic("info-hash-triage oracle must not run downstream consumers")
}

func (l *crawlerInfoHashTriageDeliveryLane) callCount() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.calls
}

type crawlerInfoHashTriageFilterRuntimeStep struct {
	result  []protocol.ID
	err     error
	release <-chan struct{}
}

type crawlerInfoHashTriageBlockingManager struct {
	mutex      sync.Mutex
	steps      []crawlerInfoHashTriageFilterRuntimeStep
	calls      [][]protocol.ID
	called     chan int
	blockCalls int
	flushCalls int
}

func (m *crawlerInfoHashTriageBlockingManager) Filter(
	ctx context.Context,
	hashes []protocol.ID,
) ([]protocol.ID, error) {
	m.mutex.Lock()
	call := len(m.calls)
	m.calls = append(m.calls, append([]protocol.ID{}, hashes...))
	if call >= len(m.steps) {
		m.mutex.Unlock()
		return nil, fmt.Errorf("unexpected Filter call %d", call+1)
	}
	step := m.steps[call]
	m.mutex.Unlock()
	m.called <- call + 1
	if step.release != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-step.release:
		}
	}
	return append([]protocol.ID{}, step.result...), step.err
}

func (m *crawlerInfoHashTriageBlockingManager) Block(
	context.Context,
	[]protocol.ID,
	bool,
) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.blockCalls++
	return nil
}

func (m *crawlerInfoHashTriageBlockingManager) Flush(context.Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.flushCalls++
	return nil
}

func (m *crawlerInfoHashTriageBlockingManager) snapshot() ([][]protocol.ID, int, int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	calls := make([][]protocol.ID, 0, len(m.calls))
	for _, call := range m.calls {
		calls = append(calls, append([]protocol.ID{}, call...))
	}
	return calls, m.blockCalls, m.flushCalls
}

type crawlerInfoHashTriageHarness struct {
	t          *testing.T
	batch      *crawlerInfoHashTriageBatchLane
	getPeers   *crawlerInfoHashTriageDeliveryLane
	scrape     *crawlerInfoHashTriageDeliveryLane
	manager    *crawlerInfoHashTriageBlockingManager
	mock       sqlmock.Sqlmock
	sqlDB      interface{ Close() error }
	queryCalls *atomic.Int64
	cancel     context.CancelFunc
	done       chan struct{}
	stopOnce   sync.Once
}

func newCrawlerInfoHashTriageHarness(
	t *testing.T,
	steps []crawlerInfoHashTriageFilterRuntimeStep,
	getPeersCapacity int,
	scrapeCapacity int,
) *crawlerInfoHashTriageHarness {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: sqlDB, PreferSimpleProtocol: true,
	}), &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent), DisableAutomaticPing: true})
	if err != nil {
		_ = sqlDB.Close()
		t.Fatal(err)
	}
	queryCalls := &atomic.Int64{}
	if err := db.Callback().Query().Before("gorm:query").Register(
		"info_hash_triage_oracle:count_query",
		func(*gorm.DB) { queryCalls.Add(1) },
	); err != nil {
		_ = sqlDB.Close()
		t.Fatal(err)
	}
	batch := newCrawlerInfoHashTriageBatchLane()
	getPeers := newCrawlerInfoHashTriageDeliveryLane(getPeersCapacity)
	scrape := newCrawlerInfoHashTriageDeliveryLane(scrapeCapacity)
	manager := &crawlerInfoHashTriageBlockingManager{steps: steps, called: make(chan int, 16)}
	ctx, cancel := context.WithCancel(context.Background())
	h := &crawlerInfoHashTriageHarness{
		t: t, batch: batch, getPeers: getPeers, scrape: scrape, manager: manager,
		mock: mock, sqlDB: sqlDB, queryCalls: queryCalls, cancel: cancel, done: make(chan struct{}),
	}
	c := crawler{
		infoHashTriage: batch, getPeers: getPeers, scrape: scrape,
		blockingManager: manager, dao: dao.Use(db),
		saveFilesThreshold: 100, rescrapeThreshold: 30 * 24 * time.Hour,
		logger: zap.NewNop().Sugar(),
	}
	go func() {
		c.runInfoHashTriage(ctx)
		close(h.done)
	}()
	t.Cleanup(func() { h.stop() })
	return h
}

func (h *crawlerInfoHashTriageHarness) stop() {
	h.stopOnce.Do(func() {
		h.cancel()
		select {
		case <-h.done:
		case <-time.After(2 * time.Second):
			h.t.Error("runInfoHashTriage did not return after cancellation")
		}
		h.mock.ExpectClose()
		if err := h.sqlDB.Close(); err != nil {
			h.t.Errorf("close sqlmock database: %v", err)
		}
		if err := h.mock.ExpectationsWereMet(); err != nil {
			h.t.Errorf("sqlmock expectations: %v", err)
		}
	})
}

func (h *crawlerInfoHashTriageHarness) waitFilterCall(want int) {
	h.t.Helper()
	select {
	case got := <-h.manager.called:
		if got != want {
			h.t.Fatalf("Filter call = %d, want %d", got, want)
		}
	case <-time.After(2 * time.Second):
		h.t.Fatalf("timed out waiting for Filter call %d", want)
	}
}

func (h *crawlerInfoHashTriageHarness) sendBatch(requests ...nodeHasPeersForHash) {
	h.t.Helper()
	select {
	case h.batch.output <- append([]nodeHasPeersForHash{}, requests...):
	case <-time.After(2 * time.Second):
		h.t.Fatal("timed out submitting triage batch")
	}
}

func (h *crawlerInfoHashTriageHarness) waitLaneAccess(lane string, want int) {
	h.t.Helper()
	var accessed <-chan int
	switch lane {
	case "get_peers":
		accessed = h.getPeers.accessed
	case "scrape":
		accessed = h.scrape.accessed
	default:
		h.t.Fatalf("unknown lane %q", lane)
	}
	select {
	case got := <-accessed:
		if got != want {
			h.t.Fatalf("%s In call = %d, want %d", lane, got, want)
		}
	case <-time.After(2 * time.Second):
		h.t.Fatalf("timed out waiting for %s In call %d", lane, want)
	}
}

func (h *crawlerInfoHashTriageHarness) assertQueryCount(want int) {
	h.t.Helper()
	if got := h.queryCalls.Load(); got != int64(want) {
		h.t.Fatalf("database query calls = %d, want %d", got, want)
	}
}

func crawlerInfoHashTriageID(value byte) protocol.ID {
	var id protocol.ID
	id[len(id)-1] = value
	return id
}

func crawlerInfoHashTriageRequestValue(hashValue, nodeValue byte) nodeHasPeersForHash {
	return nodeHasPeersForHash{
		infoHash: crawlerInfoHashTriageID(hashValue),
		node:     netip.MustParseAddrPort(fmt.Sprintf("192.0.2.%d:%d", nodeValue, 7000+int(nodeValue))),
	}
}

func crawlerInfoHashTriageFixtureRequest(request nodeHasPeersForHash) crawlerInfoHashTriageRequest {
	return crawlerInfoHashTriageRequest{InfoHash: request.infoHash.String(), Node: request.node.String()}
}

func crawlerInfoHashTriageIDs(values ...byte) []protocol.ID {
	ids := make([]protocol.ID, 0, len(values))
	for _, value := range values {
		ids = append(ids, crawlerInfoHashTriageID(value))
	}
	return ids
}

func crawlerInfoHashTriageIDStrings(ids []protocol.ID) []string {
	values := make([]string, 0, len(ids))
	for _, id := range ids {
		values = append(values, id.String())
	}
	return values
}

func crawlerInfoHashTriageRuntimeOracle(determinism string) crawlerInfoHashTriageOracle {
	return crawlerInfoHashTriageOracle{
		Composition: "actual_crawler_runInfoHashTriage_with_manual_interface_lanes_scripted_blocking_Manager_and_sqlmock_DAO",
		Determinism: determinism,
		Database:    "actual_GORM_DAO_query_over_sqlmock_without_live_Postgres",
		Clock:       "production_time_Now_with_runtime_rows_far_from_the_staleness_boundary",
	}
}

func crawlerInfoHashTriageBaseInput(kind string) crawlerInfoHashTriageInput {
	return crawlerInfoHashTriageInput{
		Kind: kind, Batches: [][]crawlerInfoHashTriageRequest{},
		FilterSteps: []crawlerInfoHashTriageFilterStep{}, DatabaseRows: []crawlerInfoHashTriageDatabaseRow{},
		SaveFiles: 100, RescrapeSeconds: int64((30 * 24 * time.Hour) / time.Second),
	}
}

func crawlerInfoHashTriageBaseExpected() crawlerInfoHashTriageExpected {
	return crawlerInfoHashTriageExpected{
		FilterCalls: [][]string{}, SQLArgs: []string{}, Actions: []crawlerInfoHashTriageAction{},
	}
}

func crawlerInfoHashTriageActionValue(action string, request nodeHasPeersForHash) crawlerInfoHashTriageAction {
	return crawlerInfoHashTriageAction{Action: action, InfoHash: request.infoHash.String(), Node: request.node.String()}
}

func sortCrawlerInfoHashTriageActions(actions []crawlerInfoHashTriageAction) {
	sort.Slice(actions, func(i, j int) bool {
		left := actions[i].Action + ":" + actions[i].InfoHash + ":" + actions[i].Node
		right := actions[j].Action + ":" + actions[j].InfoHash + ":" + actions[j].Node
		return left < right
	})
}

func TestGenerateDHTCrawlerInfoHashTriageParity(t *testing.T) {
	fixtures := []crawlerInfoHashTriageFixture{
		crawlerInfoHashTriageSourceFixture(t),
		crawlerInfoHashTriageDecisionMatrixFixture(t),
		crawlerInfoHashTriageEmptyFilterFixture(t),
		crawlerInfoHashTriageFilterErrorFixture(t),
		crawlerInfoHashTriageDatabaseErrorFixture(t),
		crawlerInfoHashTriageBlockedGetPeersFixture(t),
		crawlerInfoHashTriageBlockedScrapeFixture(t),
	}
	if len(fixtures) != len(crawlerInfoHashTriageFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(crawlerInfoHashTriageFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerInfoHashTriageFixtureIDs[index] {
			t.Fatalf("fixture %d ID = %q, want %q", index, fixture.ID, crawlerInfoHashTriageFixtureIDs[index])
		}
		if fixture.Subsystem != "dht_crawler_info_hash_triage" {
			t.Fatalf("fixture %s subsystem = %q, want dht_crawler_info_hash_triage", fixture.ID, fixture.Subsystem)
		}
		wantClassification := "RUNTIME_EXACT"
		if index == 0 {
			wantClassification = "SOURCE_ONLY"
		}
		if fixture.Classification != wantClassification {
			t.Fatalf("fixture %s classification = %q, want %q", fixture.ID, fixture.Classification, wantClassification)
		}
	}
	reconcileCrawlerInfoHashTriageFixtures(t, fixtures)
}

func crawlerInfoHashTriageSourceFixture(t *testing.T) crawlerInfoHashTriageFixture {
	t.Helper()
	assertCrawlerInfoHashTriageSourceShapes(t)
	config := NewDefaultConfig()
	if config.ScalingFactor != 10 || config.SaveFilesThreshold != 100 ||
		config.RescrapeThreshold != 30*24*time.Hour {
		t.Fatalf("unexpected DHT crawler defaults: %+v", config)
	}
	return crawlerInfoHashTriageFixture{
		ID: "production_source_factory_and_lifecycle_contract", Subsystem: "dht_crawler_info_hash_triage",
		Classification: "SOURCE_ONLY",
		Oracle: crawlerInfoHashTriageOracle{
			Composition: "exact_Go_source_factory_configuration_model_DAO_and_channel_freshness_gate",
			Determinism: "source_SHA256_plus_required_AST_and_factory_shapes",
			Database:    "source_contract_only_without_live_Postgres",
			Clock:       "source_contract_only_for_production_time_Now",
		},
		Input: crawlerInfoHashTriageBaseInput("source_contract"),
		Expected: crawlerInfoHashTriageExpected{
			FilterCalls: [][]string{}, SQLArgs: []string{}, Actions: []crawlerInfoHashTriageAction{},
			RunReturned: false,
			Source: &crawlerInfoHashTriageSource{
				InputCapacity: 100, BatchLimit: 1000, BatchIntervalSeconds: 20, BatchOutputCapacity: 1,
				GetPeersInputCapacity: 100, GetPeersConcurrency: 200,
				ScrapeInputCapacity: 100, ScrapeConcurrency: 200,
				DefaultScalingFactor: 10, DefaultSaveFilesThreshold: 100,
				DefaultRescrapeThresholdSeconds: int64((30 * 24 * time.Hour) / time.Second),
				FirstDuplicateWins:              true, FilterReceivesFirstUniqueOrder: true, FilterBeforeDatabase: true,
				FilteredHashesDedupedForRouting: true, FilteredDuplicatesRemainSQLArgs: true,
				DatabaseDuplicateLastWins: true,
				SelectedColumns:           []string{"torrents.info_hash", "torrents.files_status", "torrents.files_count", "torrents_torrent_sources.seeders", "torrents_torrent_sources.leechers", "torrents_torrent_sources.updated_at"},
				JoinKind:                  "left_join", JoinSource: "dht", GetPeersPrecedesScrape: true,
				StrictStaleBefore: true, TimeNowReadPerReachedStalenessCheck: true,
				ErrorBreakContinuesOuterLoop: true, SendsCancellationAware: true,
				ClosedOutChecksOpenBoolean: false, WorkerDetached: true, WorkerJoined: false, NoStats: true,
				NormalizedASTSHA256: crawlerInfoHashTriageNormalizedASTDigests(t),
				SourceSHA256:        crawlerInfoHashTriageSourceDigests(t),
				GoModSQLMockLine:    crawlerInfoHashTriageDependencyLine(t, "go.mod", "github.com/DATA-DOG/go-sqlmock "),
				GoSumSQLMockLine:    crawlerInfoHashTriageDependencyLine(t, "go.sum", "github.com/DATA-DOG/go-sqlmock v1.5.2 "),
				Evidence:            "exact source freshness plus behavioral rows executing crawler.runInfoHashTriage with real DAO query construction",
				Nonclaims: []string{
					"map iteration SQL result and downstream delivery order",
					"exact rescrape boundary behavior or wall-clock determinism",
					"live PostgreSQL schema query plan indexes or result ordering",
					"production blocking bloom state buffering or flush behavior",
					"production batching timer input close or output close behavior",
					"downstream consumer callbacks concurrency semaphore or completion",
					"select tie resolution scheduling fairness or side effects beyond recorded downstream In accessor evaluation",
					"log messages levels fields or delivery",
					"total work retention throughput or backpressure capacity",
					"closed infoHashTriage output behavior because production does not check receive openness",
					"end-to-end live DHT traffic network peers or external services",
					"upstream sample_infohashes response origin responding-node address has-peers or ignore-hash provenance",
					"Rust implementation API statistics supervision application wiring deployment or production readiness",
				},
			},
		},
	}
}

func crawlerInfoHashTriageDecisionMatrixFixture(t *testing.T) crawlerInfoHashTriageFixture {
	t.Helper()
	requests := []nodeHasPeersForHash{
		crawlerInfoHashTriageRequestValue(1, 1), crawlerInfoHashTriageRequestValue(1, 11),
		crawlerInfoHashTriageRequestValue(2, 2), crawlerInfoHashTriageRequestValue(3, 3),
		crawlerInfoHashTriageRequestValue(4, 4), crawlerInfoHashTriageRequestValue(5, 5),
		crawlerInfoHashTriageRequestValue(6, 6), crawlerInfoHashTriageRequestValue(7, 7),
		crawlerInfoHashTriageRequestValue(8, 8), crawlerInfoHashTriageRequestValue(9, 9),
		crawlerInfoHashTriageRequestValue(10, 10),
	}
	filtered := crawlerInfoHashTriageIDs(1, 3, 4, 5, 6, 7, 8, 9, 10)
	old := time.Unix(0, 0).UTC()
	fresh := time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)
	rows := []crawlerInfoHashTriageRuntimeDatabaseRow{
		{InfoHash: crawlerInfoHashTriageID(3), FilesStatus: model.FilesStatusNoInfo, UpdatedAt: fresh},
		{InfoHash: crawlerInfoHashTriageID(4), FilesStatus: model.FilesStatusMulti, UpdatedAt: fresh},
		{InfoHash: crawlerInfoHashTriageID(5), FilesStatus: model.FilesStatusOverThreshold, FilesCount: crawlerInfoHashTriageUint(100), Seeders: crawlerInfoHashTriageUint(4), Leechers: crawlerInfoHashTriageUint(5), UpdatedAt: fresh},
		{InfoHash: crawlerInfoHashTriageID(6), FilesStatus: model.FilesStatusSingle, Seeders: nil, Leechers: nil, UpdatedAt: fresh},
		{InfoHash: crawlerInfoHashTriageID(7), FilesStatus: model.FilesStatusMulti, FilesCount: crawlerInfoHashTriageUint(1), Seeders: crawlerInfoHashTriageUint(6), Leechers: crawlerInfoHashTriageUint(7), UpdatedAt: old},
		{InfoHash: crawlerInfoHashTriageID(8), FilesStatus: model.FilesStatusSingle, Seeders: crawlerInfoHashTriageUint(8), Leechers: crawlerInfoHashTriageUint(9), UpdatedAt: fresh},
		{InfoHash: crawlerInfoHashTriageID(9), FilesStatus: model.FilesStatusOverThreshold, FilesCount: crawlerInfoHashTriageUint(101), Seeders: crawlerInfoHashTriageUint(10), Leechers: crawlerInfoHashTriageUint(11), UpdatedAt: fresh},
		{InfoHash: crawlerInfoHashTriageID(10), FilesStatus: model.FilesStatusOverThreshold, Seeders: crawlerInfoHashTriageUint(12), Leechers: crawlerInfoHashTriageUint(13), UpdatedAt: fresh},
	}
	barrier := crawlerInfoHashTriageRequestValue(30, 30)
	h := newCrawlerInfoHashTriageHarness(t, []crawlerInfoHashTriageFilterRuntimeStep{
		{result: filtered}, {result: []protocol.ID{}},
	}, 16, 16)
	h.expectQuery(filtered, rows, nil)
	h.sendBatch(requests...)
	h.waitFilterCall(1)
	actions := h.collectActions(7)
	h.sendBatch(barrier)
	h.waitFilterCall(2)
	wantActions := []crawlerInfoHashTriageAction{
		crawlerInfoHashTriageActionValue("get_peers", requests[0]),
		crawlerInfoHashTriageActionValue("get_peers", requests[3]),
		crawlerInfoHashTriageActionValue("get_peers", requests[4]),
		crawlerInfoHashTriageActionValue("get_peers", requests[5]),
		crawlerInfoHashTriageActionValue("scrape", requests[6]),
		crawlerInfoHashTriageActionValue("scrape", requests[7]),
		crawlerInfoHashTriageActionValue("get_peers", requests[10]),
	}
	sortCrawlerInfoHashTriageActions(wantActions)
	if !reflect.DeepEqual(actions, wantActions) {
		t.Fatalf("actions = %#v, want %#v", actions, wantActions)
	}
	h.assertNoQueuedActions()
	h.stop()
	h.assertQueryCount(1)
	filterCalls, blockCalls, flushCalls := h.manager.snapshot()
	wantFilterCalls := [][]protocol.ID{
		crawlerInfoHashTriageIDs(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), {barrier.infoHash},
	}
	if !reflect.DeepEqual(filterCalls, wantFilterCalls) {
		t.Fatalf("Filter calls = %#v, want %#v", filterCalls, wantFilterCalls)
	}
	input := crawlerInfoHashTriageBaseInput("dedup_filter_query_and_route")
	input.Batches = [][]crawlerInfoHashTriageRequest{
		crawlerInfoHashTriageFixtureRequests(requests), {crawlerInfoHashTriageFixtureRequest(barrier)},
	}
	input.FilterSteps = []crawlerInfoHashTriageFilterStep{
		{Result: crawlerInfoHashTriageIDStrings(filtered)}, {Result: []string{}},
	}
	input.DatabaseRows = crawlerInfoHashTriageFixtureDatabaseRows(rows)
	expected := crawlerInfoHashTriageBaseExpected()
	expected.FilterCalls = crawlerInfoHashTriageFixtureFilterCalls(wantFilterCalls)
	expected.SQLArgs = append([]string{"dht"}, crawlerInfoHashTriageIDStrings(filtered)...)
	expected.DatabaseQueryCalls = 1
	expected.Actions = actions
	expected.GetPeersInCalls = 5
	expected.ScrapeInCalls = 2
	expected.BlockCalls = blockCalls
	expected.FlushCalls = flushCalls
	expected.RunReturned = true
	expected.ContextCancelled = true
	return crawlerInfoHashTriageFixture{
		ID: "dedup_filter_lookup_and_decision_matrix", Subsystem: "dht_crawler_info_hash_triage",
		Classification: "RUNTIME_EXACT", Oracle: crawlerInfoHashTriageRuntimeOracle("sorted_action_multiset_with_fixed_far_boundary_rows"),
		Input: input, Expected: expected,
	}
}

func crawlerInfoHashTriageEmptyFilterFixture(t *testing.T) crawlerInfoHashTriageFixture {
	t.Helper()
	request := crawlerInfoHashTriageRequestValue(20, 20)
	barrier := crawlerInfoHashTriageRequestValue(31, 31)
	h := newCrawlerInfoHashTriageHarness(t, []crawlerInfoHashTriageFilterRuntimeStep{
		{result: []protocol.ID{}}, {result: []protocol.ID{}},
	}, 1, 1)
	h.sendBatch(request)
	h.waitFilterCall(1)
	h.sendBatch(barrier)
	h.waitFilterCall(2)
	h.assertNoQueuedActions()
	h.stop()
	h.assertQueryCount(0)
	calls, blockCalls, flushCalls := h.manager.snapshot()
	input := crawlerInfoHashTriageBaseInput("empty_filter_result")
	input.Batches = [][]crawlerInfoHashTriageRequest{
		{crawlerInfoHashTriageFixtureRequest(request)}, {crawlerInfoHashTriageFixtureRequest(barrier)},
	}
	input.FilterSteps = []crawlerInfoHashTriageFilterStep{{Result: []string{}}, {Result: []string{}}}
	expected := crawlerInfoHashTriageBaseExpected()
	expected.FilterCalls = crawlerInfoHashTriageFixtureFilterCalls(calls)
	expected.BlockCalls, expected.FlushCalls = blockCalls, flushCalls
	expected.RunReturned, expected.ContextCancelled = true, true
	return crawlerInfoHashTriageFixture{
		ID: "empty_filter_result_skips_database_and_outputs", Subsystem: "dht_crawler_info_hash_triage",
		Classification: "RUNTIME_EXACT", Oracle: crawlerInfoHashTriageRuntimeOracle("independent_GORM_query_counter_zero_and_no_queued_action"),
		Input: input, Expected: expected,
	}
}

func crawlerInfoHashTriageFilterErrorFixture(t *testing.T) crawlerInfoHashTriageFixture {
	t.Helper()
	first := crawlerInfoHashTriageRequestValue(21, 21)
	second := crawlerInfoHashTriageRequestValue(22, 22)
	sentinel := errors.New("oracle filter failure")
	h := newCrawlerInfoHashTriageHarness(t, []crawlerInfoHashTriageFilterRuntimeStep{
		{err: sentinel}, {result: []protocol.ID{}},
	}, 1, 1)
	h.sendBatch(first)
	h.waitFilterCall(1)
	h.sendBatch(second)
	h.waitFilterCall(2)
	h.assertNoQueuedActions()
	h.stop()
	h.assertQueryCount(0)
	calls, blockCalls, flushCalls := h.manager.snapshot()
	input := crawlerInfoHashTriageBaseInput("filter_error_then_continue")
	input.Batches = [][]crawlerInfoHashTriageRequest{{crawlerInfoHashTriageFixtureRequest(first)}, {crawlerInfoHashTriageFixtureRequest(second)}}
	input.FilterSteps = []crawlerInfoHashTriageFilterStep{{Error: sentinel.Error(), Result: []string{}}, {Result: []string{}}}
	expected := crawlerInfoHashTriageBaseExpected()
	expected.FilterCalls = crawlerInfoHashTriageFixtureFilterCalls(calls)
	expected.BlockCalls, expected.FlushCalls = blockCalls, flushCalls
	expected.RunReturned, expected.ContextCancelled, expected.ContinuedAfterErr = true, true, true
	return crawlerInfoHashTriageFixture{
		ID: "filter_error_drops_batch_and_continues", Subsystem: "dht_crawler_info_hash_triage",
		Classification: "RUNTIME_EXACT", Oracle: crawlerInfoHashTriageRuntimeOracle("two_observed_filter_calls_with_independent_GORM_query_counter_zero"),
		Input: input, Expected: expected,
	}
}

func crawlerInfoHashTriageDatabaseErrorFixture(t *testing.T) crawlerInfoHashTriageFixture {
	t.Helper()
	first := crawlerInfoHashTriageRequestValue(23, 23)
	second := crawlerInfoHashTriageRequestValue(24, 24)
	sentinel := errors.New("oracle database failure")
	firstResult := []protocol.ID{first.infoHash}
	h := newCrawlerInfoHashTriageHarness(t, []crawlerInfoHashTriageFilterRuntimeStep{
		{result: firstResult}, {result: []protocol.ID{}},
	}, 1, 1)
	h.expectQuery(firstResult, nil, sentinel)
	h.sendBatch(first)
	h.waitFilterCall(1)
	h.sendBatch(second)
	h.waitFilterCall(2)
	h.assertNoQueuedActions()
	h.stop()
	h.assertQueryCount(1)
	calls, blockCalls, flushCalls := h.manager.snapshot()
	input := crawlerInfoHashTriageBaseInput("database_error_then_continue")
	input.Batches = [][]crawlerInfoHashTriageRequest{{crawlerInfoHashTriageFixtureRequest(first)}, {crawlerInfoHashTriageFixtureRequest(second)}}
	input.FilterSteps = []crawlerInfoHashTriageFilterStep{{Result: crawlerInfoHashTriageIDStrings(firstResult)}, {Result: []string{}}}
	input.DatabaseError = sentinel.Error()
	expected := crawlerInfoHashTriageBaseExpected()
	expected.FilterCalls = crawlerInfoHashTriageFixtureFilterCalls(calls)
	expected.SQLArgs = append([]string{"dht"}, crawlerInfoHashTriageIDStrings(firstResult)...)
	expected.DatabaseQueryCalls = 1
	expected.BlockCalls, expected.FlushCalls = blockCalls, flushCalls
	expected.RunReturned, expected.ContextCancelled, expected.ContinuedAfterErr = true, true, true
	return crawlerInfoHashTriageFixture{
		ID: "database_error_drops_batch_and_continues", Subsystem: "dht_crawler_info_hash_triage",
		Classification: "RUNTIME_EXACT", Oracle: crawlerInfoHashTriageRuntimeOracle("one_independently_counted_expected_query_then_second_observed_filter_call"),
		Input: input, Expected: expected,
	}
}

func crawlerInfoHashTriageBlockedGetPeersFixture(t *testing.T) crawlerInfoHashTriageFixture {
	t.Helper()
	request := crawlerInfoHashTriageRequestValue(25, 25)
	filtered := []protocol.ID{request.infoHash}
	h := newCrawlerInfoHashTriageHarness(t, []crawlerInfoHashTriageFilterRuntimeStep{{result: filtered}}, 0, 1)
	h.expectQuery(filtered, nil, nil)
	h.sendBatch(request)
	h.waitFilterCall(1)
	h.waitLaneAccess("get_peers", 1)
	h.stop()
	h.assertQueryCount(1)
	if h.getPeers.callCount() != 1 || h.scrape.callCount() != 0 {
		t.Fatalf("lane calls = get_peers:%d scrape:%d, want 1:0", h.getPeers.callCount(), h.scrape.callCount())
	}
	calls, blockCalls, flushCalls := h.manager.snapshot()
	input := crawlerInfoHashTriageBaseInput("cancel_blocked_send")
	input.Batches = [][]crawlerInfoHashTriageRequest{{crawlerInfoHashTriageFixtureRequest(request)}}
	input.FilterSteps = []crawlerInfoHashTriageFilterStep{{Result: crawlerInfoHashTriageIDStrings(filtered)}}
	input.CancelAtLane = "get_peers"
	expected := crawlerInfoHashTriageBaseExpected()
	expected.FilterCalls = crawlerInfoHashTriageFixtureFilterCalls(calls)
	expected.SQLArgs = append([]string{"dht"}, crawlerInfoHashTriageIDStrings(filtered)...)
	expected.DatabaseQueryCalls = 1
	expected.GetPeersInCalls = 1
	expected.BlockCalls, expected.FlushCalls = blockCalls, flushCalls
	expected.RunReturned, expected.ContextCancelled = true, true
	return crawlerInfoHashTriageFixture{
		ID: "cancellation_at_blocked_get_peers_send", Subsystem: "dht_crawler_info_hash_triage",
		Classification: "RUNTIME_EXACT", Oracle: crawlerInfoHashTriageRuntimeOracle("one_independently_counted_query_then_unbuffered_send_access_observed_before_cancel_and_join"),
		Input: input, Expected: expected,
	}
}

func crawlerInfoHashTriageBlockedScrapeFixture(t *testing.T) crawlerInfoHashTriageFixture {
	t.Helper()
	request := crawlerInfoHashTriageRequestValue(26, 26)
	filtered := []protocol.ID{request.infoHash}
	fresh := time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)
	rows := []crawlerInfoHashTriageRuntimeDatabaseRow{{
		InfoHash: request.infoHash, FilesStatus: model.FilesStatusSingle,
		Seeders: nil, Leechers: nil, UpdatedAt: fresh,
	}}
	h := newCrawlerInfoHashTriageHarness(t, []crawlerInfoHashTriageFilterRuntimeStep{{result: filtered}}, 1, 0)
	h.expectQuery(filtered, rows, nil)
	h.sendBatch(request)
	h.waitFilterCall(1)
	h.waitLaneAccess("scrape", 1)
	h.stop()
	h.assertQueryCount(1)
	if h.getPeers.callCount() != 0 || h.scrape.callCount() != 1 {
		t.Fatalf("lane calls = get_peers:%d scrape:%d, want 0:1", h.getPeers.callCount(), h.scrape.callCount())
	}
	calls, blockCalls, flushCalls := h.manager.snapshot()
	input := crawlerInfoHashTriageBaseInput("cancel_blocked_send")
	input.Batches = [][]crawlerInfoHashTriageRequest{{crawlerInfoHashTriageFixtureRequest(request)}}
	input.FilterSteps = []crawlerInfoHashTriageFilterStep{{Result: crawlerInfoHashTriageIDStrings(filtered)}}
	input.DatabaseRows = crawlerInfoHashTriageFixtureDatabaseRows(rows)
	input.CancelAtLane = "scrape"
	expected := crawlerInfoHashTriageBaseExpected()
	expected.FilterCalls = crawlerInfoHashTriageFixtureFilterCalls(calls)
	expected.SQLArgs = append([]string{"dht"}, crawlerInfoHashTriageIDStrings(filtered)...)
	expected.DatabaseQueryCalls = 1
	expected.ScrapeInCalls = 1
	expected.BlockCalls, expected.FlushCalls = blockCalls, flushCalls
	expected.RunReturned, expected.ContextCancelled = true, true
	return crawlerInfoHashTriageFixture{
		ID: "cancellation_at_blocked_scrape_send", Subsystem: "dht_crawler_info_hash_triage",
		Classification: "RUNTIME_EXACT", Oracle: crawlerInfoHashTriageRuntimeOracle("one_independently_counted_query_then_unbuffered_send_access_observed_before_cancel_and_join"),
		Input: input, Expected: expected,
	}
}

func crawlerInfoHashTriageUint(value uint) *uint { return &value }

func crawlerInfoHashTriageFixtureRequests(requests []nodeHasPeersForHash) []crawlerInfoHashTriageRequest {
	result := make([]crawlerInfoHashTriageRequest, 0, len(requests))
	for _, request := range requests {
		result = append(result, crawlerInfoHashTriageFixtureRequest(request))
	}
	return result
}

func crawlerInfoHashTriageFixtureFilterCalls(calls [][]protocol.ID) [][]string {
	result := make([][]string, 0, len(calls))
	for _, call := range calls {
		result = append(result, crawlerInfoHashTriageIDStrings(call))
	}
	return result
}

func crawlerInfoHashTriageFixtureDatabaseRows(
	rows []crawlerInfoHashTriageRuntimeDatabaseRow,
) []crawlerInfoHashTriageDatabaseRow {
	result := make([]crawlerInfoHashTriageDatabaseRow, 0, len(rows))
	for _, row := range rows {
		result = append(result, crawlerInfoHashTriageDatabaseRow{
			InfoHash: row.InfoHash.String(), FilesStatus: row.FilesStatus.String(), FilesCount: row.FilesCount,
			Seeders: row.Seeders, Leechers: row.Leechers, UpdatedAt: row.UpdatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	return result
}

func crawlerInfoHashTriageDriverUint(value *uint) driver.Value {
	if value == nil {
		return nil
	}
	return int64(*value)
}

func crawlerInfoHashTriageSQL(hashCount int) string {
	if hashCount == 1 {
		return `SELECT "torrents"."info_hash","torrents"."files_status","torrents"."files_count","torrents_torrent_sources"."seeders","torrents_torrent_sources"."leechers","torrents_torrent_sources"."updated_at" FROM "torrents" LEFT JOIN "torrents_torrent_sources" ON "torrents"."info_hash" = "torrents_torrent_sources"."info_hash" AND "torrents_torrent_sources"."source" = $1 WHERE "torrents"."info_hash" = $2`
	}
	parameters := make([]string, hashCount)
	for index := range parameters {
		parameters[index] = fmt.Sprintf("$%d", index+2)
	}
	return `SELECT "torrents"."info_hash","torrents"."files_status","torrents"."files_count","torrents_torrent_sources"."seeders","torrents_torrent_sources"."leechers","torrents_torrent_sources"."updated_at" FROM "torrents" LEFT JOIN "torrents_torrent_sources" ON "torrents"."info_hash" = "torrents_torrent_sources"."info_hash" AND "torrents_torrent_sources"."source" = $1 WHERE "torrents"."info_hash" IN (` + strings.Join(parameters, ",") + `)`
}

func (h *crawlerInfoHashTriageHarness) expectQuery(
	hashes []protocol.ID,
	rows []crawlerInfoHashTriageRuntimeDatabaseRow,
	queryErr error,
) {
	h.t.Helper()
	arguments := make([]driver.Value, 0, len(hashes)+1)
	arguments = append(arguments, "dht")
	for _, hash := range hashes {
		arguments = append(arguments, append([]byte{}, hash.Bytes()...))
	}
	expectation := h.mock.ExpectQuery(crawlerInfoHashTriageSQL(len(hashes))).WithArgs(arguments...)
	if queryErr != nil {
		expectation.WillReturnError(queryErr)
		return
	}
	mockRows := sqlmock.NewRows([]string{
		"info_hash", "files_status", "files_count", "seeders", "leechers", "updated_at",
	})
	for _, row := range rows {
		mockRows.AddRow(
			append([]byte{}, row.InfoHash.Bytes()...), string(row.FilesStatus),
			crawlerInfoHashTriageDriverUint(row.FilesCount), crawlerInfoHashTriageDriverUint(row.Seeders),
			crawlerInfoHashTriageDriverUint(row.Leechers), row.UpdatedAt,
		)
	}
	expectation.WillReturnRows(mockRows).RowsWillBeClosed()
}

func (h *crawlerInfoHashTriageHarness) collectActions(want int) []crawlerInfoHashTriageAction {
	h.t.Helper()
	actions := make([]crawlerInfoHashTriageAction, 0, want)
	for len(actions) < want {
		select {
		case request := <-h.getPeers.input:
			actions = append(actions, crawlerInfoHashTriageActionValue("get_peers", request))
		case request := <-h.scrape.input:
			actions = append(actions, crawlerInfoHashTriageActionValue("scrape", request))
		case <-time.After(2 * time.Second):
			h.t.Fatalf("timed out collecting action %d of %d", len(actions)+1, want)
		}
	}
	sortCrawlerInfoHashTriageActions(actions)
	return actions
}

func (h *crawlerInfoHashTriageHarness) assertNoQueuedActions() {
	h.t.Helper()
	select {
	case request := <-h.getPeers.input:
		h.t.Fatalf("unexpected get_peers action: %#v", request)
	case request := <-h.scrape.input:
		h.t.Fatalf("unexpected scrape action: %#v", request)
	default:
	}
}

func assertCrawlerInfoHashTriageSourceShapes(t *testing.T) {
	t.Helper()
	root := crawlerInfoHashTriageRoot(t)
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, filepath.Join(root, "internal/dhtcrawler/infohash_triage.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var function *ast.FuncDecl
	for _, declaration := range file.Decls {
		candidate, ok := declaration.(*ast.FuncDecl)
		if ok && candidate.Name.Name == "runInfoHashTriage" {
			function = candidate
			break
		}
	}
	if function == nil {
		t.Fatal("runInfoHashTriage function not found")
	}
	var formatted bytes.Buffer
	if err := format.Node(&formatted, fileSet, function); err != nil {
		t.Fatal(err)
	}
	source := formatted.String()
	required := []string{
		"case reqs := <-c.infoHashTriage.Out():",
		"if _, ok := reqMap[r.infoHash]; ok {\n\t\t\t\t\tcontinue\n\t\t\t\t}",
		"filteredHashes, filterErr := c.blockingManager.Filter(ctx, allHashes)",
		"if filterErr != nil {", "if len(filteredHashes) == 0 {",
		"filteredHashMap[h] = struct{}{}", "valuers = append(valuers, h)",
		"c.dao.TorrentsTorrentSource.Source.Eq(\"dht\")",
		"foundTorrents[t.InfoHash] = *t",
		"t.FilesStatus == model.FilesStatusNoInfo",
		"t.FilesStatus != model.FilesStatusSingle && !t.FilesCount.Valid",
		"t.FilesStatus == model.FilesStatusOverThreshold && t.FilesCount.Uint <= c.saveFilesThreshold",
		"case c.getPeers.In() <- r:",
		"!t.Seeders.Valid || !t.Leechers.Valid",
		"t.UpdatedAt.Before(time.Now().Add(-c.rescrapeThreshold))",
		"case c.scrape.In() <- r:",
	}
	for _, snippet := range required {
		if !strings.Contains(source, snippet) {
			t.Fatalf("runInfoHashTriage missing required source shape %q", snippet)
		}
	}
	factory := crawlerInfoHashTriageReadFile(t, "internal/dhtcrawler/factory.go")
	for _, snippet := range []string{
		"10*scalingFactor, 1000, 20*time.Second",
		"getPeers: concurrency.NewBufferedConcurrentChannel[nodeHasPeersForHash](\n\t\t\t\t\t\t\t10*scalingFactor, 20*scalingFactor)",
		"scrape: concurrency.NewBufferedConcurrentChannel[nodeHasPeersForHash](\n\t\t\t\t\t\t\t10*scalingFactor, 20*scalingFactor)",
	} {
		if !strings.Contains(factory, snippet) {
			t.Fatalf("factory missing required source shape %q", snippet)
		}
	}
	crawlerSource := crawlerInfoHashTriageReadFile(t, "internal/dhtcrawler/crawler.go")
	if !strings.Contains(crawlerSource, "go c.runInfoHashTriage(ctx)") ||
		strings.Contains(crawlerSource, "go func() {\n\t\tc.runInfoHashTriage(ctx)") {
		t.Fatal("info-hash triage worker is no longer directly detached from crawler.start")
	}
	batching := crawlerInfoHashTriageReadFile(t, "internal/concurrency/batching_channel.go")
	if !strings.Contains(batching, "output:       make(chan []T, 1)") {
		t.Fatal("production batching output capacity is no longer one")
	}
}

func crawlerInfoHashTriageNormalizedASTDigests(t *testing.T) map[string]string {
	t.Helper()
	specifications := []struct {
		key  string
		path string
		kind string
		name string
	}{
		{key: "config.NewDefaultConfig", path: "internal/dhtcrawler/config.go", kind: "func", name: "NewDefaultConfig"},
		{key: "crawler.nodeHasPeersForHash", path: "internal/dhtcrawler/crawler.go", kind: "type", name: "nodeHasPeersForHash"},
		{key: "crawler.start", path: "internal/dhtcrawler/crawler.go", kind: "func", name: "start"},
		{key: "factory.New", path: "internal/dhtcrawler/factory.go", kind: "func", name: "New"},
		{key: "infohash.triageResult", path: "internal/dhtcrawler/infohash_triage.go", kind: "type", name: "triageResult"},
		{key: "infohash.runInfoHashTriage", path: "internal/dhtcrawler/infohash_triage.go", kind: "func", name: "runInfoHashTriage"},
	}
	digests := make(map[string]string, len(specifications))
	for _, specification := range specifications {
		fileSet := token.NewFileSet()
		path := filepath.Join(crawlerInfoHashTriageRoot(t), specification.path)
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var node ast.Node
		for _, declaration := range file.Decls {
			switch typed := declaration.(type) {
			case *ast.FuncDecl:
				if specification.kind == "func" && typed.Name.Name == specification.name {
					node = typed
				}
			case *ast.GenDecl:
				if specification.kind != "type" {
					continue
				}
				for _, rawSpec := range typed.Specs {
					typeSpec, ok := rawSpec.(*ast.TypeSpec)
					if ok && typeSpec.Name.Name == specification.name {
						node = typeSpec
					}
				}
			}
		}
		if node == nil {
			t.Fatalf("%s %s not found in %s", specification.kind, specification.name, specification.path)
		}
		var normalized bytes.Buffer
		if err := format.Node(&normalized, fileSet, node); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(normalized.Bytes())
		actual := fmt.Sprintf("%x", digest)
		expected, ok := crawlerInfoHashTriageExpectedNormalizedASTSHA256[specification.key]
		if !ok || expected == "" {
			t.Fatalf("missing expected normalized AST SHA-256 for %s", specification.key)
		}
		if actual != expected {
			t.Fatalf("normalized AST SHA-256 %s = %s, want %s", specification.key, actual, expected)
		}
		digests[specification.key] = actual
	}
	return digests
}

func crawlerInfoHashTriageSourceDigests(t *testing.T) map[string]string {
	t.Helper()
	paths := []string{
		"internal/blocking/manager.go",
		"internal/concurrency/batching_channel.go",
		"internal/concurrency/buffered_concurrent_channel.go",
		"internal/database/dao/torrents.gen.go",
		"internal/database/dao/torrents_torrent_sources.gen.go",
		"internal/dhtcrawler/config.go",
		"internal/dhtcrawler/crawler.go",
		"internal/dhtcrawler/factory.go",
		"internal/dhtcrawler/infohash_triage.go",
		"internal/dhtcrawler/sample_infohashes.go",
		"internal/model/files_status_enum.go",
		"internal/model/null.go",
		"internal/model/torrents.gen.go",
		"internal/model/torrents_torrent_sources.gen.go",
		"internal/protocol/id.go",
	}
	digests := make(map[string]string, len(paths))
	for _, path := range paths {
		contents := []byte(crawlerInfoHashTriageReadFile(t, path))
		digest := sha256.Sum256(contents)
		digests[path] = fmt.Sprintf("%x", digest)
	}
	return digests
}

func crawlerInfoHashTriageDependencyLine(t *testing.T, path string, prefix string) string {
	t.Helper()
	for _, line := range strings.Split(crawlerInfoHashTriageReadFile(t, path), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			return strings.TrimSpace(line)
		}
	}
	t.Fatalf("dependency line with prefix %q not found in %s", prefix, path)
	return ""
}

func crawlerInfoHashTriageReadFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(crawlerInfoHashTriageRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func crawlerInfoHashTriageRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve info-hash-triage generator source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
}

func reconcileCrawlerInfoHashTriageFixtures(t *testing.T, fixtures []crawlerInfoHashTriageFixture) {
	t.Helper()
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	for _, fixture := range fixtures {
		if err := encoder.Encode(fixture); err != nil {
			t.Fatal(err)
		}
	}
	digest := sha256.Sum256(encoded.Bytes())
	actualHash := fmt.Sprintf("%x", digest)
	if crawlerInfoHashTriageFixtureSHA256 != "" && actualHash != crawlerInfoHashTriageFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerInfoHashTriageFixtureSHA256)
	}
	path := filepath.Join(crawlerInfoHashTriageRoot(t), "testdata/parity/dht/dht_crawler_info_hash_triage.jsonl")
	if *updateDHTCrawlerInfoHashTriageParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-info-hash-triage-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler info-hash-triage fixture is stale; rerun with -update-dht-crawler-info-hash-triage-parity")
	}
}

var (
	_ concurrency.BatchingChannel[nodeHasPeersForHash]           = (*crawlerInfoHashTriageBatchLane)(nil)
	_ concurrency.BufferedConcurrentChannel[nodeHasPeersForHash] = (*crawlerInfoHashTriageDeliveryLane)(nil)
	_ blocking.Manager                                           = (*crawlerInfoHashTriageBlockingManager)(nil)
)
