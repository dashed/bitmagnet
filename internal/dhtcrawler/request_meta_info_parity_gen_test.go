package dhtcrawler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	ami "github.com/anacrolix/torrent/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/banning"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
)

var updateDHTCrawlerRequestMetaInfoParity = flag.Bool(
	"update-dht-crawler-request-meta-info-parity", false,
	"rewrite the DHT crawler request-metainfo parity fixture",
)

const crawlerRequestMetaInfoFixtureSHA256 = "03ce2ab0da2b0f9ba1173b8ba52481a903265ca6862f957b40490cf67a9e4ec5"

var crawlerRequestMetaInfoFixtureIDs = [...]string{
	"production_source_factory_and_lifecycle_contract",
	"zero_peers_returns_nil_error_and_emits_zero_parsed_info",
	"ordered_duplicate_peers_fail_through_to_first_allowed_hybrid_success",
	"all_peer_failures_join_in_attempt_order_and_preserve_causes",
	"banned_success_invokes_block_hash_false_ignores_block_error_stops_and_emits_none",
	"cancellation_during_first_request_error_continues_remaining_peers_with_same_cancelled_context",
	"cancelled_before_scripted_success_still_checks_ban_and_eagerly_evaluates_unavailable_persist_in",
	"lane_error_is_swallowed",
}

var crawlerRequestMetaInfoExpectedNormalizedASTSHA256 = map[string]string{
	"banning.Checker":                       "4e63f1a6ec946417983d103e70b3bcd1f7ca28a2363ab616d99970ea528f135e",
	"banning.New":                           "be3d2ed77f1c448fbd5c439cf8074d9af7fa6fc318c625a56149361c17080ac9",
	"banning.combinedChecker.Check":         "3d7e6507567670469050ea30493667d02ebaa3c65836b972187fa2aacb95b092",
	"batching.In":                           "f5ef939724dc08bc0fa39e9fa2e0863e45acd1c965609ad91fa7082fd6632b21",
	"batching.NewBatchingChannel":           "2c9a3fa894f82680a8cb8437d8dbad6d3bc2da9a7594c83553ef7650dd472dc6",
	"batching.Out":                          "f677733fd65c621331747365d30bc29503cda90a21e5aba68ece706afd5d2e3c",
	"blocking.Manager":                      "d4a130c8c8f8414c0522de3abfa7438c405b0ed93b6703e2945af5b4a83d250f",
	"buffered.In":                           "47b8d0cda8a3039f6d0ea101430404511705d63aafe3ea9edf95e7883f17bedb",
	"buffered.NewBufferedConcurrentChannel": "562428750b1aaf7a4811758daa63468461d995ac00f36e4d7b620fedfb4633ec",
	"buffered.Run":                          "0a8f90020ab24fb50cad498fcf376777cde3b5f6aa6424da3e66b15b54e3292f",
	"crawler.infoHashWithMetaInfo":          "7de701e7f26b3dbbe7f82adc220ec88ffc362afd476bf5899fe20401afa0ce6d",
	"crawler.infoHashWithPeers":             "9effbfa014d73da2f826c0c78a8388c8260ff76474f12d47cbab434303bf345e",
	"crawler.nodeHasPeersForHash":           "1e2206b038dd5c1b70dff5a29cdf044ad7133b4876db75723081ab37c3d3da58",
	"crawler.start":                         "d61a318ce626352ee4f5cd5dd48191d767bbfe45b6a9def673cd185eada4f67b",
	"factory.New":                           "0204a00fd63b275339d63d622865858571c153bc81fc738784a78e1c150fec80",
	"metainfo.ParsedInfo":                   "51664f615ffbaff8382bef86eadfc7d0b1c722acfd76b2ca86705a921d3065d0",
	"requester.Requester":                   "f57bed7d9fea486c6fa441a1576432f9ec03a0914e79784b2b6092e810dd76dd",
	"requester.Response":                    "4b09076bf112c4fc5f81987da2fc81450f18cd6c5aed01ea5daf4e29b8e4cab1",
	"requestmeta.doRequestMetaInfo":         "f8ea6b497cfe359c313660b37c251a6396a83a186e8f83a42f0571ca0a901ca5",
	"requestmeta.runRequestMetaInfo":        "97bde956993ae99f1b52b5eac40e95da84b53a3ee10e1f7f16d6f0c0c8b54b91",
}

type crawlerRequestMetaInfoFixture struct {
	ID             string                         `json:"id"`
	Subsystem      string                         `json:"subsystem"`
	Classification string                         `json:"classification"`
	Oracle         crawlerRequestMetaInfoOracle   `json:"oracle"`
	Input          crawlerRequestMetaInfoInput    `json:"input"`
	Expected       crawlerRequestMetaInfoExpected `json:"expected"`
}

type crawlerRequestMetaInfoOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Lane        string `json:"lane"`
	Requester   string `json:"requester"`
	Banning     string `json:"banning"`
	Blocking    string `json:"blocking"`
	Handoff     string `json:"handoff"`
}

type crawlerRequestMetaInfoInput struct {
	Kind                  string                                   `json:"kind"`
	Request               *crawlerRequestMetaInfoRequest           `json:"request,omitempty"`
	Outcomes              []crawlerRequestMetaInfoRequesterOutcome `json:"outcomes"`
	BanError              string                                   `json:"banError"`
	BlockError            string                                   `json:"blockError"`
	CancelRequesterAtCall int                                      `json:"cancelRequesterAtCall"`
	BlockerPending        bool                                     `json:"blockerPending"`
	HandoffMode           string                                   `json:"handoffMode"`
	HandoffCapacity       int                                      `json:"handoffCapacity"`
	CancelAtHandoffInCall int                                      `json:"cancelAtHandoffInCall"`
	LaneReturnError       bool                                     `json:"laneReturnError"`
}

type crawlerRequestMetaInfoRequest struct {
	InfoHash string                          `json:"infoHash"`
	Node     crawlerRequestMetaInfoAddress   `json:"node"`
	Peers    []crawlerRequestMetaInfoAddress `json:"peers"`
}

type crawlerRequestMetaInfoAddress struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope"`
}

type crawlerRequestMetaInfoRequesterOutcome struct {
	Kind        string `json:"kind"`
	Error       string `json:"error"`
	Name        string `json:"name"`
	MetaVersion uint8  `json:"metaVersion"`
	InfoHashV1  string `json:"infoHashV1"`
	InfoHashV2  string `json:"infoHashV2"`
	InvalidInfo bool   `json:"invalidInfo"`
}

type crawlerRequestMetaInfoExpected struct {
	RequesterCalls    []crawlerRequestMetaInfoRequesterCall `json:"requesterCalls"`
	SameContext       bool                                  `json:"sameContext"`
	BanningCalls      []string                              `json:"banningCalls"`
	BanningErrors     []string                              `json:"banningErrors"`
	BlockCalls        []crawlerRequestMetaInfoBlockCall     `json:"blockCalls"`
	HandoffInCalls    int                                   `json:"handoffInCalls"`
	HandoffDeliveries []crawlerRequestMetaInfoHandoff       `json:"handoffDeliveries"`
	Events            []string                              `json:"events"`
	DoResult          *crawlerRequestMetaInfoResult         `json:"doResult,omitempty"`
	DoError           string                                `json:"doError"`
	DoErrorIdentities []bool                                `json:"doErrorIdentities"`
	RunReturned       bool                                  `json:"runReturned"`
	ContextCancelled  bool                                  `json:"contextCancelled"`
	CallbackCompleted bool                                  `json:"callbackCompleted"`
	Source            *crawlerRequestMetaInfoSource         `json:"source,omitempty"`
}

type crawlerRequestMetaInfoRequesterCall struct {
	InfoHash         string                        `json:"infoHash"`
	Peer             crawlerRequestMetaInfoAddress `json:"peer"`
	ContextCancelled bool                          `json:"contextCancelled"`
}

type crawlerRequestMetaInfoBlockCall struct {
	Hashes []string `json:"hashes"`
	Flush  bool     `json:"flush"`
}

type crawlerRequestMetaInfoResult struct {
	Name        string `json:"name"`
	MetaVersion uint8  `json:"metaVersion"`
	InfoHashV1  string `json:"infoHashV1"`
	InfoHashV2  string `json:"infoHashV2"`
}

type crawlerRequestMetaInfoHandoff struct {
	InfoHash    string                        `json:"infoHash"`
	Node        crawlerRequestMetaInfoAddress `json:"node"`
	Name        string                        `json:"name"`
	MetaVersion uint8                         `json:"metaVersion"`
	InfoHashV1  string                        `json:"infoHashV1"`
	InfoHashV2  string                        `json:"infoHashV2"`
}

type crawlerRequestMetaInfoSource struct {
	RunErrorIgnored                    bool              `json:"runErrorIgnored"`
	SharedCallbackContext              bool              `json:"sharedCallbackContext"`
	PeersAttemptedSequentially         bool              `json:"peersAttemptedSequentially"`
	PeerOrderAndDuplicatesPreserved    bool              `json:"peerOrderAndDuplicatesPreserved"`
	RequesterFailureFallsThrough       bool              `json:"requesterFailureFallsThrough"`
	FirstRequesterSuccessStops         bool              `json:"firstRequesterSuccessStops"`
	BanningCheckedOnlyAfterSuccess     bool              `json:"banningCheckedOnlyAfterSuccess"`
	BannedHashBlockFlushArgumentFalse  bool              `json:"bannedHashBlockFlushArgumentFalse"`
	BlockErrorIgnored                  bool              `json:"blockErrorIgnored"`
	BannedSuccessStops                 bool              `json:"bannedSuccessStops"`
	AllFailuresJoinedInOrder           bool              `json:"allFailuresJoinedInOrder"`
	ZeroPeersReturnsNilError           bool              `json:"zeroPeersReturnsNilError"`
	SuccessfulHandoffUsesOriginalRoute bool              `json:"successfulHandoffUsesOriginalRoute"`
	SuccessfulHandoffUsesParsedInfo    bool              `json:"successfulHandoffUsesParsedInfo"`
	PersistInEagerlyEvaluated          bool              `json:"persistInEagerlyEvaluated"`
	RunPersistTorrentsExecuted         bool              `json:"runPersistTorrentsExecuted"`
	ProductionInputCapacity            int               `json:"productionInputCapacity"`
	ProductionConcurrency              int               `json:"productionConcurrency"`
	ProductionHandoffCapacity          int               `json:"productionHandoffCapacity"`
	ProductionHandoffMaxBatchSize      int               `json:"productionHandoffMaxBatchSize"`
	ProductionHandoffIntervalMS        int               `json:"productionHandoffIntervalMs"`
	ProductionHandoffOutputCapacity    int               `json:"productionHandoffOutputCapacity"`
	DefaultScalingFactor               int               `json:"defaultScalingFactor"`
	ConsumerDequeuesBeforeSemaphore    bool              `json:"consumerDequeuesBeforeSemaphore"`
	ConsumerCallbacksDetached          bool              `json:"consumerCallbacksDetached"`
	ConsumerCallbacksJoined            bool              `json:"consumerCallbacksJoined"`
	MaximumRetainedWork                string            `json:"maximumRetainedWork"`
	ClosedInputChecksOpenBoolean       bool              `json:"closedInputChecksOpenBoolean"`
	ClosedInputOutcome                 string            `json:"closedInputOutcome"`
	StartLaunchesWorkerDetached        bool              `json:"startLaunchesWorkerDetached"`
	StartWaitsOnlyStopped              bool              `json:"startWaitsOnlyStopped"`
	StartDefersSharedContextCancel     bool              `json:"startDefersSharedContextCancel"`
	StartJoinsWorkerOrCallbacks        bool              `json:"startJoinsWorkerOrCallbacks"`
	NormalizedASTSHA256                map[string]string `json:"normalizedAstSha256"`
	PrerequisiteFixtureSHA256          map[string]string `json:"prerequisiteFixtureSha256"`
	EvidenceCommit                     map[string]string `json:"evidenceCommit"`
	SourceSHA256                       map[string]string `json:"sourceSha256"`
	Nonclaims                          []string          `json:"nonclaims"`
	Evidence                           string            `json:"evidence"`
}

type crawlerRequestMetaInfoEvents struct {
	sync.Mutex
	events []string
}

func (e *crawlerRequestMetaInfoEvents) add(event string) {
	e.Lock()
	defer e.Unlock()
	e.events = append(e.events, event)
}

func (e *crawlerRequestMetaInfoEvents) snapshot() []string {
	e.Lock()
	defer e.Unlock()
	return append([]string{}, e.events...)
}

type crawlerRequestMetaInfoManualLane struct {
	requests  []infoHashWithPeers
	returnErr error
	events    *crawlerRequestMetaInfoEvents
	completed bool
}

func (*crawlerRequestMetaInfoManualLane) In() chan<- infoHashWithPeers {
	panic("request-metainfo worker must not request its input sender")
}

func (l *crawlerRequestMetaInfoManualLane) Run(_ context.Context, callback func(infoHashWithPeers)) error {
	for index, request := range l.requests {
		l.events.add("lane_callback:" + strconv.Itoa(index+1))
		callback(request)
		l.completed = true
	}
	if l.returnErr != nil {
		l.events.add("lane_return_error")
	}
	return l.returnErr
}

type crawlerRequestMetaInfoRequester struct {
	wantContext   context.Context
	outcomes      []crawlerRequestMetaInfoRequesterOutcome
	pendingAtCall int
	entered       chan struct{}
	events        *crawlerRequestMetaInfoEvents
	calls         []crawlerRequestMetaInfoRequesterCall
	errors        []error
	sameContext   bool
}

func (r *crawlerRequestMetaInfoRequester) Request(
	ctx context.Context, hash protocol.ID, peer netip.AddrPort,
) (metainforequester.Response, error) {
	r.sameContext = r.sameContext && ctx == r.wantContext
	r.calls = append(r.calls, crawlerRequestMetaInfoRequesterCall{
		InfoHash: hash.String(), Peer: crawlerRequestMetaInfoProjectAddress(peer), ContextCancelled: ctx.Err() != nil,
	})
	r.events.add("request:" + strconv.Itoa(len(r.calls)))
	outcome := r.outcomes[len(r.calls)-1]
	if len(r.calls) == r.pendingAtCall {
		close(r.entered)
		<-ctx.Done()
		err := ctx.Err()
		r.errors = append(r.errors, err)
		return metainforequester.Response{}, err
	}
	if outcome.Kind == "error" {
		err := errors.New(outcome.Error)
		r.errors = append(r.errors, err)
		return metainforequester.Response{}, err
	}
	info := metainfo.Info{Name: outcome.Name}
	if outcome.InvalidInfo {
		info.Name = string([]byte{0xff})
	}
	parsed := metainfo.ParsedInfo{Info: info, MetaVersion: outcome.MetaVersion}
	if outcome.InfoHashV1 != "" {
		hash := protocol.MustParseID(outcome.InfoHashV1)
		parsed.InfoHashV1 = &hash
	}
	if outcome.InfoHashV2 != "" {
		decoded, err := hex.DecodeString(outcome.InfoHashV2)
		if err != nil {
			panic(err)
		}
		var hash protocol.InfoHashV2
		copy(hash[:], decoded)
		parsed.InfoHashV2 = &hash
	}
	return metainforequester.Response{ParsedInfo: parsed}, nil
}

type crawlerRequestMetaInfoChecker struct {
	events   *crawlerRequestMetaInfoEvents
	banErr   error
	delegate banning.Checker
	calls    []string
	errors   []string
}

func (c *crawlerRequestMetaInfoChecker) Check(info metainfo.Info) error {
	c.calls = append(c.calls, info.Name)
	c.events.add("ban_check:" + strconv.Itoa(len(c.calls)))
	err := c.banErr
	if c.delegate != nil {
		err = c.delegate.Check(info)
	}
	if err != nil {
		c.errors = append(c.errors, err.Error())
	}
	return err
}

type crawlerRequestMetaInfoBlocker struct {
	events   *crawlerRequestMetaInfoEvents
	blockErr error
	pending  bool
	entered  chan struct{}
	calls    []crawlerRequestMetaInfoBlockCall
}

func (*crawlerRequestMetaInfoBlocker) Filter(_ context.Context, hashes []protocol.ID) ([]protocol.ID, error) {
	return hashes, nil
}

func (b *crawlerRequestMetaInfoBlocker) Block(ctx context.Context, hashes []protocol.ID, flush bool) error {
	projected := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		projected = append(projected, hash.String())
	}
	b.calls = append(b.calls, crawlerRequestMetaInfoBlockCall{Hashes: projected, Flush: flush})
	b.events.add("block:" + strconv.Itoa(len(b.calls)))
	if b.pending {
		close(b.entered)
		<-ctx.Done()
		return ctx.Err()
	}
	return b.blockErr
}

func (*crawlerRequestMetaInfoBlocker) Flush(context.Context) error { return nil }

type crawlerRequestMetaInfoHandoffLane struct {
	input          chan infoHashWithMetaInfo
	events         *crawlerRequestMetaInfoEvents
	cancel         context.CancelFunc
	cancelAtInCall int
	sync.Mutex
	inCalls int
}

func (l *crawlerRequestMetaInfoHandoffLane) In() chan<- infoHashWithMetaInfo {
	l.Lock()
	l.inCalls++
	call := l.inCalls
	l.Unlock()
	l.events.add("persist_in:" + strconv.Itoa(call))
	if l.cancel != nil && call == l.cancelAtInCall {
		l.cancel()
	}
	return l.input
}

func (*crawlerRequestMetaInfoHandoffLane) Out() <-chan []infoHashWithMetaInfo {
	panic("request-metainfo oracle must not run persistence output")
}

func (l *crawlerRequestMetaInfoHandoffLane) snapshot() (int, []crawlerRequestMetaInfoHandoff) {
	l.Lock()
	calls := l.inCalls
	l.Unlock()
	deliveries := make([]crawlerRequestMetaInfoHandoff, 0, len(l.input))
	for len(l.input) > 0 {
		value := <-l.input
		v1, v2 := crawlerRequestMetaInfoIdentities(value.metaInfo)
		deliveries = append(deliveries, crawlerRequestMetaInfoHandoff{
			InfoHash: value.infoHash.String(), Node: crawlerRequestMetaInfoProjectAddress(value.node),
			Name: value.metaInfo.Info.Name, MetaVersion: value.metaInfo.MetaVersion, InfoHashV1: v1, InfoHashV2: v2,
		})
	}
	return calls, deliveries
}

func crawlerRequestMetaInfoIdentities(parsed metainfo.ParsedInfo) (string, string) {
	var v1, v2 string
	if parsed.InfoHashV1 != nil {
		v1 = parsed.InfoHashV1.String()
	}
	if parsed.InfoHashV2 != nil {
		v2 = parsed.InfoHashV2.String()
	}
	return v1, v2
}

type crawlerRequestMetaInfoScenario struct {
	id, classification                     string
	kind                                   string
	request                                *infoHashWithPeers
	outcomes                               []crawlerRequestMetaInfoRequesterOutcome
	banError, blockError                   string
	cancelRequesterAtCall                  int
	blockerPending                         bool
	actualDefaultBanning                   bool
	handoffMode                            string
	handoffCapacity, cancelAtHandoffInCall int
	laneReturnError                        bool
}

func TestGenerateDHTCrawlerRequestMetaInfoParity(t *testing.T) {
	requestWithHash := func(hash protocol.ID, node string, peers ...string) *infoHashWithPeers {
		value := &infoHashWithPeers{nodeHasPeersForHash: nodeHasPeersForHash{
			infoHash: hash, node: netip.MustParseAddrPort(node),
		}}
		for _, peer := range peers {
			value.peers = append(value.peers, netip.MustParseAddrPort(peer))
		}
		return value
	}
	request := func(hash int, node string, peers ...string) *infoHashWithPeers {
		return requestWithHash(crawlerPingWorkerID(hash), node, peers...)
	}
	errorOutcome := func(message string) crawlerRequestMetaInfoRequesterOutcome {
		return crawlerRequestMetaInfoRequesterOutcome{Kind: "error", Error: message}
	}
	success := func(name string, version uint8) crawlerRequestMetaInfoRequesterOutcome {
		return crawlerRequestMetaInfoRequesterOutcome{Kind: "success", Name: name, MetaVersion: version}
	}
	hybridTorrent, err := os.ReadFile("../protocol/metainfo/testdata/bittorrent-v2-hybrid-test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	hybridMeta, err := ami.Load(bytes.NewReader(hybridTorrent))
	if err != nil {
		t.Fatal(err)
	}
	hybridBytes := []byte(hybridMeta.InfoBytes)
	hybridHash := protocol.ID(ami.HashBytes(hybridBytes))
	hybridParsed, err := metainfo.ParseMetaInfoBytes(hybridHash, hybridBytes)
	if err != nil {
		t.Fatal(err)
	}
	hybridV1, hybridV2 := crawlerRequestMetaInfoIdentities(hybridParsed)
	hybridSuccess := crawlerRequestMetaInfoRequesterOutcome{Kind: "success", Name: hybridParsed.Info.Name, MetaVersion: hybridParsed.MetaVersion, InfoHashV1: hybridV1, InfoHashV2: hybridV2}
	scenarios := []crawlerRequestMetaInfoScenario{
		{
			id: crawlerRequestMetaInfoFixtureIDs[1], classification: "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA", kind: "run",
			request: request(201, "198.51.100.201:7201"), handoffMode: "buffered_accept_one", handoffCapacity: 1,
		},
		{
			id: crawlerRequestMetaInfoFixtureIDs[2], classification: "RUNTIME_EXACT", kind: "run",
			request:  requestWithHash(hybridHash, "198.51.100.202:7202", "203.0.113.1:7301", "203.0.113.1:7301", "[2001:db8::2%9]:7302", "203.0.113.3:7303"),
			outcomes: []crawlerRequestMetaInfoRequesterOutcome{errorOutcome("peer one failed"), errorOutcome("duplicate peer failed"), hybridSuccess}, handoffMode: "buffered_accept_one", handoffCapacity: 1,
		},
		{
			id: crawlerRequestMetaInfoFixtureIDs[3], classification: "RUNTIME_EXACT", kind: "do",
			request:  request(203, "198.51.100.203:7203", "203.0.113.6:7306", "[2001:db8::7%11]:7307", "203.0.113.6:7306"),
			outcomes: []crawlerRequestMetaInfoRequesterOutcome{errorOutcome("first failure"), errorOutcome("second failure"), errorOutcome("third failure")}, handoffMode: "not_executed",
		},
		{
			id: crawlerRequestMetaInfoFixtureIDs[4], classification: "RUNTIME_EXACT", kind: "run",
			request: request(204, "198.51.100.204:7204", "203.0.113.4:7304", "203.0.113.5:7305"), outcomes: []crawlerRequestMetaInfoRequesterOutcome{{Kind: "success", InvalidInfo: true}}, actualDefaultBanning: true, blockError: "oracle ignored block failure", handoffMode: "buffered_accept_one", handoffCapacity: 1,
		},
		{
			id: crawlerRequestMetaInfoFixtureIDs[5], classification: "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA", kind: "run",
			request: request(205, "198.51.100.205:7205", "203.0.113.8:7308", "203.0.113.9:7309"), outcomes: []crawlerRequestMetaInfoRequesterOutcome{{Kind: "pending_until_cancel"}, errorOutcome("remaining peer sees cancelled context")}, cancelRequesterAtCall: 1, handoffMode: "unbuffered_no_receiver",
		},
		{
			id: crawlerRequestMetaInfoFixtureIDs[6], classification: "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA", kind: "run",
			request: request(206, "198.51.100.206:7206", "203.0.113.10:7310", "203.0.113.11:7311"), outcomes: []crawlerRequestMetaInfoRequesterOutcome{{Kind: "pending_until_cancel"}, success("allowed.after.cancel", 1)}, cancelRequesterAtCall: 1, handoffMode: "unbuffered_no_receiver",
		},
		{id: crawlerRequestMetaInfoFixtureIDs[7], classification: "GO_ONLY_LANE", kind: "run", handoffMode: "unbuffered_no_receiver", laneReturnError: true},
	}
	fixtures := []crawlerRequestMetaInfoFixture{crawlerRequestMetaInfoSourceFixture(t)}
	for _, scenario := range scenarios {
		fixtures = append(fixtures, crawlerRequestMetaInfoRunScenario(t, scenario))
	}
	if len(fixtures) != len(crawlerRequestMetaInfoFixtureIDs) {
		t.Fatalf("fixture count = %d", len(fixtures))
	}
	for index, fixture := range fixtures {
		if fixture.ID != crawlerRequestMetaInfoFixtureIDs[index] {
			t.Fatalf("fixture %d id = %q", index, fixture.ID)
		}
	}
	wantClassifications := [...]string{"SOURCE_ONLY", "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA", "RUNTIME_EXACT", "RUNTIME_EXACT", "RUNTIME_EXACT", "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA", "RUNTIME_WITH_OWNED_SHUTDOWN_DELTA", "GO_ONLY_LANE"}
	for index, fixture := range fixtures {
		if fixture.Subsystem != "dht_crawler_request_meta_info" || fixture.Classification != wantClassifications[index] {
			t.Fatalf("fixture %s subsystem/classification = %q/%q", fixture.ID, fixture.Subsystem, fixture.Classification)
		}
	}
	crawlerRequestMetaInfoReconcile(t, fixtures)
}

func crawlerRequestMetaInfoSourceFixture(t *testing.T) crawlerRequestMetaInfoFixture {
	t.Helper()

	config := NewDefaultConfig()
	return crawlerRequestMetaInfoFixture{
		ID: crawlerRequestMetaInfoFixtureIDs[0], Subsystem: "dht_crawler_request_meta_info", Classification: "SOURCE_ONLY",
		Oracle: crawlerRequestMetaInfoOracle{Composition: "production_source_factory_and_lifecycle_freshness_gate", Determinism: "exact_normalized_AST_source_and_prerequisite_fixture_SHA256", Lane: "production_BufferedConcurrentChannel_source_shape", Requester: "production_metainforequester_Requester_interface", Banning: "production_banning_Checker_interface", Blocking: "production_blocking_Manager_interface", Handoff: "production_persistTorrents_BatchingChannel_input_shape_only"},
		Input:  crawlerRequestMetaInfoInput{Kind: "source_contract", Outcomes: []crawlerRequestMetaInfoRequesterOutcome{}, HandoffMode: "source_only"},
		Expected: crawlerRequestMetaInfoExpected{RequesterCalls: []crawlerRequestMetaInfoRequesterCall{}, BanningCalls: []string{}, BanningErrors: []string{}, BlockCalls: []crawlerRequestMetaInfoBlockCall{}, HandoffDeliveries: []crawlerRequestMetaInfoHandoff{}, Events: []string{}, Source: &crawlerRequestMetaInfoSource{
			RunErrorIgnored: true, SharedCallbackContext: true, PeersAttemptedSequentially: true,
			PeerOrderAndDuplicatesPreserved: true, RequesterFailureFallsThrough: true, FirstRequesterSuccessStops: true,
			BanningCheckedOnlyAfterSuccess: true, BannedHashBlockFlushArgumentFalse: true, BlockErrorIgnored: true,
			BannedSuccessStops: true, AllFailuresJoinedInOrder: true, ZeroPeersReturnsNilError: true,
			SuccessfulHandoffUsesOriginalRoute: true, SuccessfulHandoffUsesParsedInfo: true,
			PersistInEagerlyEvaluated: true, RunPersistTorrentsExecuted: false,
			ProductionInputCapacity: 10 * int(config.ScalingFactor), ProductionConcurrency: 40 * int(config.ScalingFactor),
			ProductionHandoffCapacity: 1000, ProductionHandoffMaxBatchSize: 1000, ProductionHandoffIntervalMS: 60000,
			ProductionHandoffOutputCapacity: 1, DefaultScalingFactor: int(config.ScalingFactor),
			ConsumerDequeuesBeforeSemaphore: true, ConsumerCallbacksDetached: true, ConsumerCallbacksJoined: false,
			MaximumRetainedWork: "capacity_plus_concurrency_plus_one_acquire_waiter", ClosedInputChecksOpenBoolean: false,
			ClosedInputOutcome:          "repeated_zero_value_callbacks_can_emit_zero_parsed_info_requests",
			StartLaunchesWorkerDetached: true, StartWaitsOnlyStopped: true, StartDefersSharedContextCancel: true,
			StartJoinsWorkerOrCallbacks: false, NormalizedASTSHA256: crawlerRequestMetaInfoNormalizedASTDigests(t),
			PrerequisiteFixtureSHA256: crawlerRequestMetaInfoPrerequisiteDigests(t), EvidenceCommit: map[string]string{
				"banning_checker_source": "f70352f4c540c6ba7e25f5aa9493766c5cc62f70", "metainfo_v2_parser": "86017663f1b61908dd4792786081e179f7538e81",
				"blocking_filter_oracle": "41f1e8cbe529d7a0bf464bb55011e0400d24b4e7", "get_peers_oracle": "19f568e01c637a8ae1b94f38e3db2c9f95734d8c",
				"info_hash_triage_oracle": "6aece7ac7605507aaf5ccdcc9adf2497170b071d", "request_metainfo_route": "73a4d867b41f4a4e7933d527c633b044736300c6",
			}, SourceSHA256: crawlerRequestMetaInfoSourceDigests(t), Nonclaims: []string{
				"goroutine_callback_scheduling_completion_or_order", "semaphore_or_channel_fairness",
				"closed_buffered_input_runtime_execution", "callback_join_guarantee", "ready_select_tie_winner",
				"arbitrary_side_effects_of_eagerly_evaluated_In_beyond_recorded_call_count", "send_to_closed_Go_channel_behavior",
				"metainfo_TCP_handshake_extension_piece_transfer_or_live_requester_behavior", "production_banning_checker_rules_beyond_the_actual_combined_banned_row",
				"Block_flush_false_argument_does_not_prove_real_manager_will_not_flush_when_shouldFlush_is_true", "blocking_manager_buffer_Bloom_flush_database_or_nonempty_durability", "runPersistTorrents_batching_deduplication_model_conversion_or_database_behavior",
				"batching_ticker_schedule_log_metrics_or_persisted_counter_delivery", "production_throughput_total_retention_or_waiter_fairness",
				"application_supervisor_deployment_or_production_readiness", "arbitrary_textual_IPv6_zones_runtime_rows_use_unscoped_or_numeric_scope_only",
				"Rust_public_API_owned_task_stats_or_shutdown_lifecycle_no_Rust_consumer_exists_in_this_slice",
			}, Evidence: "runtime rows execute actual runRequestMetaInfo or doRequestMetaInfo through controlled interfaces; persistTorrents is observed only at raw input and runPersistTorrents is never executed",
		}},
	}
}

func crawlerRequestMetaInfoRunScenario(t *testing.T, s crawlerRequestMetaInfoScenario) crawlerRequestMetaInfoFixture {
	t.Helper()
	events := &crawlerRequestMetaInfoEvents{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manual := &crawlerRequestMetaInfoManualLane{events: events}
	if s.request != nil {
		manual.requests = []infoHashWithPeers{*s.request}
	}
	if s.laneReturnError {
		manual.returnErr = errors.New("oracle lane failure")
	}
	requester := &crawlerRequestMetaInfoRequester{wantContext: ctx, outcomes: s.outcomes, pendingAtCall: s.cancelRequesterAtCall, events: events, sameContext: true}
	checker := &crawlerRequestMetaInfoChecker{events: events}
	if s.actualDefaultBanning {
		checker.delegate = banning.New(banning.Params{}).Checker
	}
	if s.banError != "" {
		checker.banErr = errors.New(s.banError)
	}
	blocker := &crawlerRequestMetaInfoBlocker{events: events}
	if s.blockError != "" {
		blocker.blockErr = errors.New(s.blockError)
	}
	handoff := &crawlerRequestMetaInfoHandoffLane{input: make(chan infoHashWithMetaInfo, s.handoffCapacity), events: events, cancel: cancel, cancelAtInCall: s.cancelAtHandoffInCall}
	var cancelDone chan struct{}
	if s.cancelRequesterAtCall > 0 {
		requester.entered = make(chan struct{})
		cancelDone = make(chan struct{})
		go func() { <-requester.entered; cancel(); close(cancelDone) }()
	}
	if s.blockerPending {
		blocker.pending = true
		blocker.entered = make(chan struct{})
		cancelDone = make(chan struct{})
		go func() { <-blocker.entered; cancel(); close(cancelDone) }()
	}
	c := crawler{requestMetaInfo: manual, metainfoRequester: requester, banningChecker: checker, blockingManager: blocker, persistTorrents: handoff}
	expected := crawlerRequestMetaInfoExpected{RequesterCalls: []crawlerRequestMetaInfoRequesterCall{}, BanningCalls: []string{}, BanningErrors: []string{}, BlockCalls: []crawlerRequestMetaInfoBlockCall{}, HandoffDeliveries: []crawlerRequestMetaInfoHandoff{}, Events: []string{}, SameContext: true}
	if s.kind == "do" {
		res, err := c.doRequestMetaInfo(ctx, s.request.infoHash, s.request.peers)
		v1, v2 := crawlerRequestMetaInfoIdentities(res.ParsedInfo)
		expected.DoResult = &crawlerRequestMetaInfoResult{Name: res.ParsedInfo.Info.Name, MetaVersion: res.ParsedInfo.MetaVersion, InfoHashV1: v1, InfoHashV2: v2}
		if err != nil {
			expected.DoError = err.Error()
		}
		for _, sentinel := range requester.errors {
			expected.DoErrorIdentities = append(expected.DoErrorIdentities, errors.Is(err, sentinel))
		}
	} else {
		c.runRequestMetaInfo(ctx)
		expected.RunReturned = true
		expected.CallbackCompleted = manual.completed
	}
	if cancelDone != nil {
		crawlerRequestMetaInfoWait(t, cancelDone, "cancellation gate")
	}
	expected.RequesterCalls = append(expected.RequesterCalls, requester.calls...)
	expected.SameContext = requester.sameContext
	expected.BanningCalls = append(expected.BanningCalls, checker.calls...)
	expected.BanningErrors = append(expected.BanningErrors, checker.errors...)
	expected.BlockCalls = append(expected.BlockCalls, blocker.calls...)
	expected.HandoffInCalls, expected.HandoffDeliveries = handoff.snapshot()
	expected.Events = events.snapshot()
	expected.ContextCancelled = ctx.Err() != nil
	input := crawlerRequestMetaInfoInput{Kind: s.kind, Outcomes: append([]crawlerRequestMetaInfoRequesterOutcome{}, s.outcomes...), BanError: s.banError, BlockError: s.blockError, CancelRequesterAtCall: s.cancelRequesterAtCall, BlockerPending: s.blockerPending, HandoffMode: s.handoffMode, HandoffCapacity: s.handoffCapacity, CancelAtHandoffInCall: s.cancelAtHandoffInCall, LaneReturnError: s.laneReturnError}
	if s.request != nil {
		projected := crawlerRequestMetaInfoProjectRequest(*s.request)
		input.Request = &projected
	}
	return crawlerRequestMetaInfoFixture{
		ID: s.id, Subsystem: "dht_crawler_request_meta_info", Classification: s.classification,
		Oracle: crawlerRequestMetaInfoOracle{Composition: "actual_runRequestMetaInfo_or_doRequestMetaInfo_with_manual_lane_and_scripted_collaborators", Determinism: "synchronous_peer_attempts_and_explicit_pending_cancellation_gates", Lane: "manual_in_order_callback_interface", Requester: "scripted_metainforequester_Requester", Banning: "scripted_banning_Checker", Blocking: "scripted_blocking_Manager", Handoff: s.handoffMode}, Input: input, Expected: expected,
	}
}

func crawlerRequestMetaInfoProjectRequest(value infoHashWithPeers) crawlerRequestMetaInfoRequest {
	peers := make([]crawlerRequestMetaInfoAddress, 0, len(value.peers))
	for _, peer := range value.peers {
		peers = append(peers, crawlerRequestMetaInfoProjectAddress(peer))
	}
	return crawlerRequestMetaInfoRequest{InfoHash: value.infoHash.String(), Node: crawlerRequestMetaInfoProjectAddress(value.node), Peers: peers}
}

func crawlerRequestMetaInfoProjectAddress(addr netip.AddrPort) crawlerRequestMetaInfoAddress {
	scope, _ := strconv.ParseUint(addr.Addr().Zone(), 10, 32)
	return crawlerRequestMetaInfoAddress{IP: addr.Addr().WithZone("").String(), Port: addr.Port(), Scope: uint32(scope)}
}

func crawlerRequestMetaInfoWait(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type crawlerRequestMetaInfoASTSpec struct{ key, path, kind, name string }

func crawlerRequestMetaInfoNormalizedASTDigests(t *testing.T) map[string]string {
	t.Helper()
	specs := []crawlerRequestMetaInfoASTSpec{
		{"batching.In", "internal/concurrency/batching_channel.go", "func", "In"},
		{"batching.NewBatchingChannel", "internal/concurrency/batching_channel.go", "func", "NewBatchingChannel"},
		{"batching.Out", "internal/concurrency/batching_channel.go", "func", "Out"},
		{"blocking.Manager", "internal/blocking/manager.go", "type", "Manager"},
		{"buffered.In", "internal/concurrency/buffered_concurrent_channel.go", "func", "In"},
		{"buffered.NewBufferedConcurrentChannel", "internal/concurrency/buffered_concurrent_channel.go", "func", "NewBufferedConcurrentChannel"},
		{"buffered.Run", "internal/concurrency/buffered_concurrent_channel.go", "func", "Run"},
		{"banning.Checker", "internal/protocol/metainfo/banning/checker.go", "type", "Checker"},
		{"banning.New", "internal/protocol/metainfo/banning/checker.go", "func", "New"},
		{"banning.combinedChecker.Check", "internal/protocol/metainfo/banning/checker.go", "func", "Check"},
		{"crawler.infoHashWithMetaInfo", "internal/dhtcrawler/crawler.go", "type", "infoHashWithMetaInfo"},
		{"crawler.infoHashWithPeers", "internal/dhtcrawler/crawler.go", "type", "infoHashWithPeers"},
		{"crawler.nodeHasPeersForHash", "internal/dhtcrawler/crawler.go", "type", "nodeHasPeersForHash"},
		{"crawler.start", "internal/dhtcrawler/crawler.go", "func", "start"},
		{"factory.New", "internal/dhtcrawler/factory.go", "func", "New"},
		{"metainfo.ParsedInfo", "internal/protocol/metainfo/parse.go", "type", "ParsedInfo"},
		{"requester.Requester", "internal/protocol/metainfo/metainforequester/requester.go", "type", "Requester"},
		{"requester.Response", "internal/protocol/metainfo/metainforequester/requester.go", "type", "Response"},
		{"requestmeta.doRequestMetaInfo", "internal/dhtcrawler/request_meta_info.go", "func", "doRequestMetaInfo"},
		{"requestmeta.runRequestMetaInfo", "internal/dhtcrawler/request_meta_info.go", "func", "runRequestMetaInfo"},
	}
	digests := make(map[string]string, len(specs))
	missing := false
	for _, spec := range specs {
		node, files := crawlerRequestMetaInfoFindASTNode(t, spec)
		var normalized bytes.Buffer
		if err := format.Node(&normalized, files, node); err != nil {
			t.Fatal(err)
		}
		actual := fmt.Sprintf("%x", sha256.Sum256(normalized.Bytes()))
		digests[spec.key] = actual
		expected := crawlerRequestMetaInfoExpectedNormalizedASTSHA256[spec.key]
		if expected == "" {
			missing = true
		} else if actual != expected {
			t.Fatalf("normalized AST %s = %s, want %s", spec.key, actual, expected)
		}
	}
	if missing {
		encoded, marshalErr := json.MarshalIndent(digests, "", "  ")
		if marshalErr != nil {
			t.Fatalf("marshal normalized AST digests: %v", marshalErr)
		}
		t.Fatalf("fill crawlerRequestMetaInfoExpectedNormalizedASTSHA256 with:\n%s", encoded)
	}
	return digests
}

func crawlerRequestMetaInfoFindASTNode(t *testing.T, spec crawlerRequestMetaInfoASTSpec) (ast.Node, *token.FileSet) {
	t.Helper()
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, filepath.Join(crawlerRequestMetaInfoRoot(t), spec.path), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		switch typed := declaration.(type) {
		case *ast.FuncDecl:
			if spec.kind == "func" && typed.Name.Name == spec.name {
				return typed, files
			}
		case *ast.GenDecl:
			if spec.kind != "type" {
				continue
			}
			for _, raw := range typed.Specs {
				if typeSpec, ok := raw.(*ast.TypeSpec); ok && typeSpec.Name.Name == spec.name {
					return typeSpec, files
				}
			}
		}
	}
	t.Fatalf("%s %s not found in %s", spec.kind, spec.name, spec.path)
	return nil, nil
}

func crawlerRequestMetaInfoPrerequisiteDigests(t *testing.T) map[string]string {
	t.Helper()

	want := map[string]string{
		"internal/protocol/metainfo/testdata/bittorrent-v2-hybrid-test.torrent": "8ba7575e64e9046cac74ca6523bff6445ff5c3e369d5d132607a793a1834e93f",
		"testdata/parity/dht/dht_crawler_get_peers.jsonl":                       "82b694fece9e46c05aefaab76bc05b78462bc04824bf6b83bb77eb544b7f0844",
		"testdata/parity/dht/dht_crawler_info_hash_triage.jsonl":                "52eda840f872225cc34f8cf12edc2e4621e8a1fef569abf34a50f4a3bd9896f8",
		"testdata/parity/dht/dht_info_hash_block_filter.jsonl":                  "cc17edc11e5a21fe668d1067d2cf7413643bfdc8b81b0d5e97e5830afb1a51b4",
	}
	crawlerRequestMetaInfoValidateDigests(t, want)
	return want
}

func crawlerRequestMetaInfoSourceDigests(t *testing.T) map[string]string {
	t.Helper()

	paths := []string{
		"internal/blocking/manager.go", "internal/concurrency/batching_channel.go", "internal/concurrency/buffered_concurrent_channel.go",
		"internal/dhtcrawler/config.go", "internal/dhtcrawler/crawler.go", "internal/dhtcrawler/factory.go", "internal/dhtcrawler/persist.go", "internal/dhtcrawler/request_meta_info.go",
		"internal/protocol/id.go", "internal/protocol/metainfo/metainfo.go", "internal/protocol/metainfo/parse.go", "internal/protocol/metainfo/banning/checker.go", "internal/protocol/metainfo/banning/name_length.go", "internal/protocol/metainfo/banning/size.go", "internal/protocol/metainfo/banning/utf8.go", "internal/protocol/metainfo/metainforequester/requester.go",
	}
	want := make(map[string]string, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(filepath.Join(crawlerRequestMetaInfoRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		want[path] = fmt.Sprintf("%x", sha256.Sum256(contents))
	}
	return want
}

func crawlerRequestMetaInfoValidateDigests(t *testing.T, want map[string]string) {
	t.Helper()

	for path, expected := range want {
		contents, err := os.ReadFile(filepath.Join(crawlerRequestMetaInfoRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		if actual := fmt.Sprintf("%x", sha256.Sum256(contents)); actual != expected {
			t.Fatalf("%s SHA-256 = %s, want %s", path, actual, expected)
		}
	}
}

func crawlerRequestMetaInfoRoot(t *testing.T) string {
	t.Helper()

	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve request-metainfo generator source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
}

func crawlerRequestMetaInfoReconcile(t *testing.T, fixtures []crawlerRequestMetaInfoFixture) {
	t.Helper()

	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	for _, fixture := range fixtures {
		if err := encoder.Encode(fixture); err != nil {
			t.Fatal(err)
		}
	}
	actualHash := fmt.Sprintf("%x", sha256.Sum256(encoded.Bytes()))
	if crawlerRequestMetaInfoFixtureSHA256 != "" && actualHash != crawlerRequestMetaInfoFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, crawlerRequestMetaInfoFixtureSHA256)
	}
	path := filepath.Join(crawlerRequestMetaInfoRoot(t), "testdata/parity/dht/dht_crawler_request_meta_info.jsonl")
	if *updateDHTCrawlerRequestMetaInfoParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", actualHash)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-crawler-request-meta-info-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT crawler request-metainfo fixture is stale; rerun with -update-dht-crawler-request-meta-info-parity")
	}
}

var (
	_ concurrency.BufferedConcurrentChannel[infoHashWithPeers] = (*crawlerRequestMetaInfoManualLane)(nil)
	_ metainforequester.Requester                              = (*crawlerRequestMetaInfoRequester)(nil)
	_ banning.Checker                                          = (*crawlerRequestMetaInfoChecker)(nil)
	_ blocking.Manager                                         = (*crawlerRequestMetaInfoBlocker)(nil)
	_ concurrency.BatchingChannel[infoHashWithMetaInfo]        = (*crawlerRequestMetaInfoHandoffLane)(nil)
)
