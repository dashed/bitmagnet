package blocking

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	bitmagnetbloom "github.com/bitmagnet-io/bitmagnet/internal/bloom"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

var updateDHTInfoHashBlockFilterParity = flag.Bool(
	"update-dht-info-hash-block-filter-parity",
	false,
	"rewrite the Rust DHT persistent info-hash block-filter parity fixture",
)

const dhtInfoHashBlockFilterFixtureSHA256 = "cc17edc11e5a21fe668d1067d2cf7413643bfdc8b81b0d5e97e5830afb1a51b4"

const (
	dhtInfoHashBlockFilterModulePath     = "github.com/tylertreat/BoomFilters"
	dhtInfoHashBlockFilterModuleVersion  = "v0.0.0-20210315201527-1a82519a3e43"
	dhtInfoHashBlockFilterModuleSum      = "h1:QEePdg0ty2r0t1+qwfZmQ4OOl/MB2UXIeJSpIZv56lg="
	dhtInfoHashBlockFilterModuleGoModSum = "h1:OYRfF6eb5wY9VRFkXJH8FFBi3plw2v+giaIu7P054pM="

	dhtInfoHashBlockFilterCells          = uint64(100_000_000)
	dhtInfoHashBlockFilterBitsPerCell    = uint8(2)
	dhtInfoHashBlockFilterHashFunctions  = uint64(5)
	dhtInfoHashBlockFilterDecrementCells = uint64(49)
	dhtInfoHashBlockFilterMaxCellValue   = uint8(3)
	dhtInfoHashBlockFilterPayloadBytes   = uint64(25_000_000)
	dhtInfoHashBlockFilterHeaderBytes    = 91
	dhtInfoHashBlockFilterWireBytes      = int64(25_000_091)
)

var dhtInfoHashBlockFilterFixtureIDs = [...]string{
	"production_source_storage_and_lifecycle_contract",
	"fresh_filter_single_add_wire_roundtrip",
}

type dhtInfoHashBlockFilterFixture struct {
	ID             string                         `json:"id"`
	Subsystem      string                         `json:"subsystem"`
	Classification string                         `json:"classification"`
	Oracle         dhtInfoHashBlockFilterOracle   `json:"oracle"`
	Input          dhtInfoHashBlockFilterInput    `json:"input"`
	Expected       dhtInfoHashBlockFilterExpected `json:"expected"`
}

type dhtInfoHashBlockFilterOracle struct {
	Composition string `json:"composition"`
	Determinism string `json:"determinism"`
	Database    string `json:"database"`
	Randomness  string `json:"randomness"`
}

type dhtInfoHashBlockFilterInput struct {
	Kind     string `json:"kind"`
	InfoHash string `json:"infoHash,omitempty"`
}

type dhtInfoHashBlockFilterExpected struct {
	Source *dhtInfoHashBlockFilterSource `json:"source,omitempty"`
	Wire   *dhtInfoHashBlockFilterWire   `json:"wire,omitempty"`
}

type dhtInfoHashBlockFilterSource struct {
	FilterKey                  string            `json:"filterKey"`
	BufferCapacity             int               `json:"bufferCapacity"`
	MaxBufferSize              int               `json:"maxBufferSize"`
	MaxFlushWaitSeconds        int               `json:"maxFlushWaitSeconds"`
	InputByteLength            int               `json:"inputByteLength"`
	FilterPreservesInputOrder  bool              `json:"filterPreservesInputOrder"`
	FilterPreservesDuplicates  bool              `json:"filterPreservesDuplicates"`
	BufferCheckedBeforeBloom   bool              `json:"bufferCheckedBeforeBloom"`
	FirstFilterLoadsState      bool              `json:"firstFilterLoadsState"`
	BlockDeduplicatesBuffer    bool              `json:"blockDeduplicatesBuffer"`
	EmptyFlushSkipsDatabase    bool              `json:"emptyFlushSkipsDatabase"`
	FlushTransactionMode       string            `json:"flushTransactionMode"`
	DeletePrecedesBloomLoad    bool              `json:"deletePrecedesBloomLoad"`
	SuccessOnlyStateSwap       bool              `json:"successOnlyStateSwap"`
	ShutdownFlushIfInitialized bool              `json:"shutdownFlushIfInitialized"`
	FilterTable                string            `json:"filterTable"`
	LargeObjectColumn          string            `json:"largeObjectColumn"`
	Metrics                    string            `json:"metrics"`
	ModulePath                 string            `json:"modulePath"`
	ModuleVersion              string            `json:"moduleVersion"`
	ModuleSourceSum            string            `json:"moduleSourceSum"`
	ModuleGoModSum             string            `json:"moduleGoModSum"`
	GoModRequirement           string            `json:"goModRequirement"`
	GoSumModuleLine            string            `json:"goSumModuleLine"`
	GoSumGoModLine             string            `json:"goSumGoModLine"`
	NormalizedASTSHA256        map[string]string `json:"normalizedAstSha256"`
	SourceSHA256               map[string]string `json:"sourceSha256"`
	Evidence                   string            `json:"evidence"`
	Nonclaims                  []string          `json:"nonclaims"`
}

type dhtInfoHashBlockFilterWire struct {
	Cells                           uint64                              `json:"cells"`
	BitsPerCell                     uint8                               `json:"bitsPerCell"`
	HashFunctions                   uint64                              `json:"hashFunctions"`
	DecrementCells                  uint64                              `json:"decrementCells"`
	MaxCellValue                    uint8                               `json:"maxCellValue"`
	IndexBuffer                     []uint64                            `json:"indexBuffer"`
	BucketSize                      uint8                               `json:"bucketSize"`
	BucketMax                       uint8                               `json:"bucketMax"`
	BucketCount                     uint64                              `json:"bucketCount"`
	CellPayloadBytes                uint64                              `json:"cellPayloadBytes"`
	HeaderBytes                     int                                 `json:"headerBytes"`
	HeaderHex                       string                              `json:"headerHex"`
	SerializedBytes                 int64                               `json:"serializedBytes"`
	SerializedSHA256                string                              `json:"serializedSha256"`
	HashKernel                      string                              `json:"hashKernel"`
	HashIndices                     []uint64                            `json:"hashIndices"`
	NonzeroPayloadBytes             []dhtInfoHashBlockFilterPayloadByte `json:"nonzeroPayloadBytes"`
	MemberAfterAdd                  bool                                `json:"memberAfterAdd"`
	AbsentProbeInfoHash             string                              `json:"absentProbeInfoHash"`
	AbsentProbeMember               bool                                `json:"absentProbeMember"`
	ReadBytes                       int64                               `json:"readBytes"`
	MemberAfterRoundtrip            bool                                `json:"memberAfterRoundtrip"`
	AbsentProbeMemberAfterRoundtrip bool                                `json:"absentProbeMemberAfterRoundtrip"`
	ReencodedIdentical              bool                                `json:"reencodedIdentical"`
	ReencodedSHA256                 string                              `json:"reencodedSha256"`
	RawFixtureBytesEmbedded         bool                                `json:"rawFixtureBytesEmbedded"`
}

type dhtInfoHashBlockFilterPayloadByte struct {
	PayloadOffset    uint64 `json:"payloadOffset"`
	SerializedOffset uint64 `json:"serializedOffset"`
	Value            uint8  `json:"value"`
}

func TestGenerateDHTInfoHashBlockFilterParity(t *testing.T) {
	fixtures := []dhtInfoHashBlockFilterFixture{
		dhtInfoHashBlockFilterSourceFixture(t),
		dhtInfoHashBlockFilterWireFixture(t),
	}
	if len(fixtures) != len(dhtInfoHashBlockFilterFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(dhtInfoHashBlockFilterFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != dhtInfoHashBlockFilterFixtureIDs[index] {
			t.Fatalf("fixture %d ID = %q, want %q", index, fixture.ID, dhtInfoHashBlockFilterFixtureIDs[index])
		}
		if fixture.Subsystem != "dht_info_hash_block_filter" {
			t.Fatalf("fixture %s subsystem = %q", fixture.ID, fixture.Subsystem)
		}
		classification := "RUNTIME_EXACT"
		if index == 0 {
			classification = "SOURCE_ONLY"
		}
		if fixture.Classification != classification {
			t.Fatalf("fixture %s classification = %q, want %q", fixture.ID, fixture.Classification, classification)
		}
	}
	reconcileDHTInfoHashBlockFilterFixtures(t, fixtures)
}

func dhtInfoHashBlockFilterSourceFixture(t *testing.T) dhtInfoHashBlockFilterFixture {
	t.Helper()
	root := dhtInfoHashBlockFilterRoot(t)
	moduleDir := assertDHTInfoHashBlockFilterModulePin(t, root)
	assertDHTInfoHashBlockFilterSourceShapes(t, root)
	requirement := dhtInfoHashBlockFilterModulePath + " " + dhtInfoHashBlockFilterModuleVersion
	return dhtInfoHashBlockFilterFixture{
		ID: dhtInfoHashBlockFilterFixtureIDs[0], Subsystem: "dht_info_hash_block_filter",
		Classification: "SOURCE_ONLY",
		Oracle: dhtInfoHashBlockFilterOracle{
			Composition: "exact_production_blocking_manager_factory_bloom_wrapper_migrations_and_pinned_BoomFilters_source",
			Determinism: "normalized_AST_exact_source_SHA256_module_checksums_and_dependency_lines",
			Database:    "source_contract_only_without_live_PostgreSQL_or_large_object_execution",
			Randomness:  "source_only_for_multi_add_map_order_and_random_decrement_behavior",
		},
		Input: dhtInfoHashBlockFilterInput{Kind: "source_contract"},
		Expected: dhtInfoHashBlockFilterExpected{Source: &dhtInfoHashBlockFilterSource{
			FilterKey:      blockedTorrentsBloomFilterKey,
			BufferCapacity: 1000, MaxBufferSize: 1000, MaxFlushWaitSeconds: 300,
			InputByteLength: len(protocol.ID{}), FilterPreservesInputOrder: true,
			FilterPreservesDuplicates: true, BufferCheckedBeforeBloom: true,
			FirstFilterLoadsState: true, BlockDeduplicatesBuffer: true,
			EmptyFlushSkipsDatabase: true, FlushTransactionMode: "read_write",
			DeletePrecedesBloomLoad: true, SuccessOnlyStateSwap: true,
			ShutdownFlushIfInitialized: true, FilterTable: "bloom_filters",
			LargeObjectColumn: "oid", Metrics: "none",
			ModulePath: dhtInfoHashBlockFilterModulePath, ModuleVersion: dhtInfoHashBlockFilterModuleVersion,
			ModuleSourceSum: dhtInfoHashBlockFilterModuleSum, ModuleGoModSum: dhtInfoHashBlockFilterModuleGoModSum,
			GoModRequirement:    requirement,
			GoSumModuleLine:     requirement + " " + dhtInfoHashBlockFilterModuleSum,
			GoSumGoModLine:      requirement + "/go.mod " + dhtInfoHashBlockFilterModuleGoModSum,
			NormalizedASTSHA256: dhtInfoHashBlockFilterASTDigests(t, root, moduleDir),
			SourceSHA256:        dhtInfoHashBlockFilterSourceDigests(t, root, moduleDir),
			Evidence:            "source-bound manager state machine plus a deterministic one-Add production codec row",
			Nonclaims: []string{
				"multi-add serialized bytes or digest",
				"math/rand seed sequence decrement start or decremented cells",
				"maps.Keys order or stable-filter add order for buffered hashes",
				"long-run false-positive false-negative eviction or retention sequence",
				"live PostgreSQL schema permissions transactions large-object I/O or rollback",
				"exact PostgreSQL object ID timestamp query plan round trips or driver errors",
				"cross-process flush serialization lost-update prevention or replica behavior",
				"manager runtime filtering buffering threshold timing flush errors or shutdown execution",
				"mutex fairness throughput cancellation latency or caller scheduling",
				"metrics logs retries statement timeouts health checks or observability",
				"Rust implementation API hardening lifecycle wiring deployment or production readiness",
			},
		}},
	}
}

func dhtInfoHashBlockFilterWireFixture(t *testing.T) dhtInfoHashBlockFilterFixture {
	t.Helper()
	infoHashText := "00000000000000000000000000000000000000a1"
	absentText := "00000000000000000000000000000000000000b2"
	infoHash := protocol.MustParseID(infoHashText)
	absent := protocol.MustParseID(absentText)
	filter := bitmagnetbloom.NewDefaultStableBloomFilter()
	filter.Add(infoHash[:])

	var encoded bytes.Buffer
	written, err := filter.WriteTo(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	blob := encoded.Bytes()
	if written != dhtInfoHashBlockFilterWireBytes || int64(len(blob)) != written {
		t.Fatalf("serialized bytes = written:%d len:%d, want %d", written, len(blob), dhtInfoHashBlockFilterWireBytes)
	}
	wire := decodeDHTInfoHashBlockFilterWire(t, blob)
	wire.SerializedSHA256 = fmt.Sprintf("%x", sha256.Sum256(blob))
	wire.HashKernel = "FNV-1_64; index_i=(low32(sum)+high32(sum)*i)%100_000_000"
	wire.HashIndices = []uint64{94_110_100, 95_868_049, 97_625_998, 99_383_947, 1_141_896}
	wire.MemberAfterAdd = filter.Test(infoHash[:])
	wire.AbsentProbeInfoHash = absentText
	wire.AbsentProbeMember = filter.Test(absent[:])
	wire.RawFixtureBytesEmbedded = false
	assertDHTInfoHashBlockFilterPayload(t, blob, &wire)

	roundtrip := bitmagnetbloom.NewDefaultStableBloomFilter()
	read, err := roundtrip.ReadFrom(bytes.NewReader(blob))
	if err != nil {
		t.Fatal(err)
	}
	wire.ReadBytes = read
	wire.MemberAfterRoundtrip = roundtrip.Test(infoHash[:])
	wire.AbsentProbeMemberAfterRoundtrip = roundtrip.Test(absent[:])
	var reencoded bytes.Buffer
	rewritten, err := roundtrip.WriteTo(&reencoded)
	if err != nil {
		t.Fatal(err)
	}
	wire.ReencodedIdentical = rewritten == written && bytes.Equal(reencoded.Bytes(), blob)
	wire.ReencodedSHA256 = fmt.Sprintf("%x", sha256.Sum256(reencoded.Bytes()))

	if wire.Cells != dhtInfoHashBlockFilterCells || wire.BitsPerCell != dhtInfoHashBlockFilterBitsPerCell ||
		wire.HashFunctions != dhtInfoHashBlockFilterHashFunctions || wire.DecrementCells != dhtInfoHashBlockFilterDecrementCells ||
		wire.MaxCellValue != dhtInfoHashBlockFilterMaxCellValue || wire.CellPayloadBytes != dhtInfoHashBlockFilterPayloadBytes ||
		wire.HeaderBytes != dhtInfoHashBlockFilterHeaderBytes || len(wire.IndexBuffer) != int(dhtInfoHashBlockFilterHashFunctions) ||
		!wire.MemberAfterAdd || wire.AbsentProbeMember || read != written || !wire.MemberAfterRoundtrip ||
		wire.AbsentProbeMemberAfterRoundtrip || !wire.ReencodedIdentical || wire.ReencodedSHA256 != wire.SerializedSHA256 {
		t.Fatalf("unexpected persistent filter wire contract: %+v", wire)
	}

	return dhtInfoHashBlockFilterFixture{
		ID: dhtInfoHashBlockFilterFixtureIDs[1], Subsystem: "dht_info_hash_block_filter",
		Classification: "RUNTIME_EXACT",
		Oracle: dhtInfoHashBlockFilterOracle{
			Composition: "actual_internal_bloom_default_StableBloomFilter_single_Add_WriteTo_ReadFrom_WriteTo",
			Determinism: "fresh_zero_cells_make_the_single_random_decrement_observationally_inert",
			Database:    "no_database_raw_BoomFilters_stream_wire_only",
			Randomness:  "one_random_decrement_occurs_but_cannot_change_any_initially_zero_payload_cell",
		},
		Input:    dhtInfoHashBlockFilterInput{Kind: "fresh_filter_single_add_wire_roundtrip", InfoHash: infoHashText},
		Expected: dhtInfoHashBlockFilterExpected{Wire: &wire},
	}
}

func decodeDHTInfoHashBlockFilterWire(t *testing.T, blob []byte) dhtInfoHashBlockFilterWire {
	t.Helper()
	reader := bytes.NewReader(blob)
	read := func(value any) {
		t.Helper()
		if err := binary.Read(reader, binary.BigEndian, value); err != nil {
			t.Fatal(err)
		}
	}
	var wire dhtInfoHashBlockFilterWire
	read(&wire.Cells)
	read(&wire.DecrementCells)
	read(&wire.HashFunctions)
	read(&wire.MaxCellValue)
	var indexBufferLength int64
	read(&indexBufferLength)
	if indexBufferLength < 0 || indexBufferLength > 1024 {
		t.Fatalf("invalid index buffer length %d", indexBufferLength)
	}
	wire.IndexBuffer = make([]uint64, indexBufferLength)
	for index := range wire.IndexBuffer {
		read(&wire.IndexBuffer[index])
	}
	read(&wire.BucketSize)
	read(&wire.BucketMax)
	read(&wire.BucketCount)
	read(&wire.CellPayloadBytes)
	wire.BitsPerCell = wire.BucketSize
	wire.HeaderBytes = len(blob) - reader.Len()
	wire.HeaderHex = hex.EncodeToString(blob[:wire.HeaderBytes])
	wire.SerializedBytes = int64(len(blob))
	if uint64(reader.Len()) != wire.CellPayloadBytes {
		t.Fatalf("remaining payload bytes = %d, header says %d", reader.Len(), wire.CellPayloadBytes)
	}
	return wire
}

func assertDHTInfoHashBlockFilterPayload(t *testing.T, blob []byte, wire *dhtInfoHashBlockFilterWire) {
	t.Helper()
	want := map[uint64]uint8{
		285_474: 3, 23_527_525: 3, 23_967_012: 12, 24_406_499: 48, 24_845_986: 192,
	}
	payload := blob[wire.HeaderBytes:]
	for offset, value := range payload {
		if value == 0 {
			continue
		}
		wantValue, ok := want[uint64(offset)]
		if !ok || wantValue != value {
			t.Fatalf("unexpected nonzero payload byte offset=%d value=%d", offset, value)
		}
		wire.NonzeroPayloadBytes = append(wire.NonzeroPayloadBytes, dhtInfoHashBlockFilterPayloadByte{
			PayloadOffset: uint64(offset), SerializedOffset: uint64(wire.HeaderBytes) + uint64(offset), Value: value,
		})
	}
	if len(wire.NonzeroPayloadBytes) != len(want) {
		t.Fatalf("nonzero payload byte count = %d, want %d", len(wire.NonzeroPayloadBytes), len(want))
	}
	for offset, value := range want {
		if payload[offset] != value {
			t.Fatalf("payload byte offset=%d value=%d, want %d", offset, payload[offset], value)
		}
	}
	// The payload scan is naturally offset ordered; make that part of the fixture.
	for index := 1; index < len(wire.NonzeroPayloadBytes); index++ {
		if wire.NonzeroPayloadBytes[index-1].PayloadOffset >= wire.NonzeroPayloadBytes[index].PayloadOffset {
			t.Fatal("nonzero payload bytes are not strictly ordered")
		}
	}
}

func assertDHTInfoHashBlockFilterSourceShapes(t *testing.T, root string) {
	t.Helper()
	required := map[string][]string{
		"internal/blocking/manager.go": {
			`const blockedTorrentsBloomFilterKey = "blocked_torrents"`,
			`DELETE FROM torrents WHERE info_hash = any($1)`,
			`SELECT oid FROM bloom_filters WHERE key = $1`,
			`m.buffer = make(map[protocol.ID]struct{})`,
			`m.filter = bf`,
			`m.lastFlushedAt = now`,
		},
		"internal/blocking/factory.go": {
			`buffer:        make(map[protocol.ID]struct{}, 1000)`,
			`maxBufferSize: 1000`,
			`maxFlushWait:  time.Minute * 5`,
			`return m.Flush(ctx)`,
		},
		"internal/bloom/stable.go": {
			`defaultCapacity = 100_000_000`,
			`defaultD        = 2`,
			`defaultFpRate   = 0.001`,
		},
		"migrations/00005_bloom_filters.sql":              {`create table bloom_filters`, `key           text`, `bytes         bytea`},
		"migrations/00020_bloom_filters_large_object.sql": {`add column oid oid`, `lo_from_bytea(0, bytes::bytea)`, `drop column bytes`},
	}
	for name, snippets := range required {
		contents, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		for _, snippet := range snippets {
			if !strings.Contains(string(contents), snippet) {
				t.Fatalf("%s missing source contract %q", name, snippet)
			}
		}
	}
}

func assertDHTInfoHashBlockFilterModulePin(t *testing.T, root string) string {
	t.Helper()
	type moduleMetadata struct {
		Path, Version, Sum, GoModSum, Dir string
		Replace                           *moduleMetadata
	}
	command := exec.Command("go", "list", "-m", "-json", dhtInfoHashBlockFilterModulePath)
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatalf("resolve BoomFilters module: %v", err)
	}
	var module moduleMetadata
	if err := json.Unmarshal(output, &module); err != nil {
		t.Fatal(err)
	}
	if module.Path != dhtInfoHashBlockFilterModulePath || module.Version != dhtInfoHashBlockFilterModuleVersion ||
		module.Sum != dhtInfoHashBlockFilterModuleSum || module.GoModSum != dhtInfoHashBlockFilterModuleGoModSum ||
		module.Dir == "" || module.Replace != nil {
		t.Fatalf("BoomFilters module metadata = %+v", module)
	}
	for path, line := range map[string]string{
		"go.mod": "\t" + dhtInfoHashBlockFilterModulePath + " " + dhtInfoHashBlockFilterModuleVersion,
		"go.sum": dhtInfoHashBlockFilterModulePath + " " + dhtInfoHashBlockFilterModuleVersion + " " + dhtInfoHashBlockFilterModuleSum,
	} {
		contents, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(contents), line+"\n") {
			t.Fatalf("%s missing %q", path, line)
		}
	}
	goSum, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	goModSumLine := dhtInfoHashBlockFilterModulePath + " " + dhtInfoHashBlockFilterModuleVersion + "/go.mod " + dhtInfoHashBlockFilterModuleGoModSum
	if !strings.Contains(string(goSum), goModSumLine+"\n") {
		t.Fatalf("go.sum missing %q", goModSumLine)
	}
	return module.Dir
}

func dhtInfoHashBlockFilterASTDigests(t *testing.T, root, moduleDir string) map[string]string {
	t.Helper()
	targets := []struct{ label, path, receiver, name string }{
		{"blocking.manager.Filter", filepath.Join(root, "internal/blocking/manager.go"), "manager", "Filter"},
		{"blocking.manager.Block", filepath.Join(root, "internal/blocking/manager.go"), "manager", "Block"},
		{"blocking.manager.Flush", filepath.Join(root, "internal/blocking/manager.go"), "manager", "Flush"},
		{"blocking.manager.flush", filepath.Join(root, "internal/blocking/manager.go"), "manager", "flush"},
		{"blocking.manager.shouldFlush", filepath.Join(root, "internal/blocking/manager.go"), "manager", "shouldFlush"},
		{"blocking.New", filepath.Join(root, "internal/blocking/factory.go"), "", "New"},
		{"bloom.NewDefaultStableBloomFilter", filepath.Join(root, "internal/bloom/stable.go"), "", "NewDefaultStableBloomFilter"},
		{"lazy.lazy.IfInitialized", filepath.Join(root, "internal/lazy/lazy.go"), "lazy", "IfInitialized"},
		{"boom.hashKernel", filepath.Join(moduleDir, "boom.go"), "", "hashKernel"},
		{"boom.NewStableBloomFilter", filepath.Join(moduleDir, "stable.go"), "", "NewStableBloomFilter"},
		{"boom.StableBloomFilter.Test", filepath.Join(moduleDir, "stable.go"), "StableBloomFilter", "Test"},
		{"boom.StableBloomFilter.Add", filepath.Join(moduleDir, "stable.go"), "StableBloomFilter", "Add"},
		{"boom.StableBloomFilter.WriteTo", filepath.Join(moduleDir, "stable.go"), "StableBloomFilter", "WriteTo"},
		{"boom.StableBloomFilter.ReadFrom", filepath.Join(moduleDir, "stable.go"), "StableBloomFilter", "ReadFrom"},
		{"boom.Buckets.WriteTo", filepath.Join(moduleDir, "buckets.go"), "Buckets", "WriteTo"},
		{"boom.Buckets.ReadFrom", filepath.Join(moduleDir, "buckets.go"), "Buckets", "ReadFrom"},
	}
	digests := make(map[string]string, len(targets))
	for _, target := range targets {
		digests[target.label] = dhtInfoHashBlockFilterFunctionDigest(t, target.path, target.receiver, target.name)
	}
	return digests
}

func dhtInfoHashBlockFilterFunctionDigest(t *testing.T, path, receiver, name string) string {
	t.Helper()
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != name || dhtInfoHashBlockFilterReceiverName(function) != receiver {
			continue
		}
		var normalized bytes.Buffer
		if err := format.Node(&normalized, set, function); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%x", sha256.Sum256(normalized.Bytes()))
	}
	t.Fatalf("function %s.%s not found in %s", receiver, name, path)
	return ""
}

func dhtInfoHashBlockFilterReceiverName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) != 1 {
		return ""
	}
	expression := function.Recv.List[0].Type
	if pointer, ok := expression.(*ast.StarExpr); ok {
		expression = pointer.X
	}
	if index, ok := expression.(*ast.IndexExpr); ok {
		expression = index.X
	}
	if index, ok := expression.(*ast.IndexListExpr); ok {
		expression = index.X
	}
	if identifier, ok := expression.(*ast.Ident); ok {
		return identifier.Name
	}
	return ""
}

func dhtInfoHashBlockFilterSourceDigests(t *testing.T, root, moduleDir string) map[string]string {
	t.Helper()
	paths := map[string]string{
		"internal/blocking/manager.go":                    filepath.Join(root, "internal/blocking/manager.go"),
		"internal/blocking/factory.go":                    filepath.Join(root, "internal/blocking/factory.go"),
		"internal/bloom/stable.go":                        filepath.Join(root, "internal/bloom/stable.go"),
		"internal/lazy/lazy.go":                           filepath.Join(root, "internal/lazy/lazy.go"),
		"migrations/00005_bloom_filters.sql":              filepath.Join(root, "migrations/00005_bloom_filters.sql"),
		"migrations/00020_bloom_filters_large_object.sql": filepath.Join(root, "migrations/00020_bloom_filters_large_object.sql"),
		dhtInfoHashBlockFilterModulePath + "@" + dhtInfoHashBlockFilterModuleVersion + "/boom.go":    filepath.Join(moduleDir, "boom.go"),
		dhtInfoHashBlockFilterModulePath + "@" + dhtInfoHashBlockFilterModuleVersion + "/buckets.go": filepath.Join(moduleDir, "buckets.go"),
		dhtInfoHashBlockFilterModulePath + "@" + dhtInfoHashBlockFilterModuleVersion + "/stable.go":  filepath.Join(moduleDir, "stable.go"),
	}
	digests := make(map[string]string, len(paths))
	for name, path := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		digests[name] = fmt.Sprintf("%x", sha256.Sum256(contents))
	}
	return digests
}

func dhtInfoHashBlockFilterRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve persistent block-filter generator source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
}

func reconcileDHTInfoHashBlockFilterFixtures(t *testing.T, fixtures []dhtInfoHashBlockFilterFixture) {
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
	if dhtInfoHashBlockFilterFixtureSHA256 != "" && digest != dhtInfoHashBlockFilterFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", digest, dhtInfoHashBlockFilterFixtureSHA256)
	}
	path := filepath.Join(dhtInfoHashBlockFilterRoot(t), "testdata/parity/dht/dht_info_hash_block_filter.jsonl")
	if *updateDHTInfoHashBlockFilterParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote fixture with SHA-256 %s", digest)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-info-hash-block-filter-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("DHT persistent info-hash block-filter fixture is stale; rerun with -update-dht-info-hash-block-filter-parity")
	}
}
