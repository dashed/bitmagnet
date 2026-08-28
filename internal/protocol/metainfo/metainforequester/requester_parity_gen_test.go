package metainforequester

import (
	"bufio"
	"bytes"
	"crypto/sha1" //nolint:gosec // BitTorrent v1 identity is defined as SHA-1.
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/peer_protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
)

var updateMetaInfoRequesterParity = flag.Bool(
	"update-metainfo-requester-parity",
	false,
	"rewrite the deterministic peer-wire metainfo requester fixture",
)

const (
	metaInfoRequesterSubsystem     = "metainfo_requester"
	metaInfoRequesterPieceSize     = 16 * 1024
	metaInfoRequesterFixtureSHA256 = "990f4d503065ed08689df37881817386874f12cda2fdaeaeb56c05e12bbcc80e"
)

var metaInfoRequesterFixtureIDs = [...]string{
	"source_contract",
	"bt_handshake_and_extension_bits",
	"extension_handshake_boundaries",
	"piece_request_and_message_boundaries",
	"piece_reader_matrix",
	"requested_hash_parse_identity",
	"controlled_go_hazards",
}

type metaInfoRequesterFixture struct {
	ID             string                    `json:"id"`
	Subsystem      string                    `json:"subsystem"`
	Classification string                    `json:"classification"`
	Execution      string                    `json:"execution"`
	Oracle         metaInfoRequesterOracle   `json:"oracle"`
	Input          metaInfoRequesterInput    `json:"input"`
	Expected       metaInfoRequesterExpected `json:"expected"`
	Nonclaims      []string                  `json:"nonclaims"`
}

type metaInfoRequesterOracle struct {
	Composition              string   `json:"composition"`
	Determinism              string   `json:"determinism"`
	InMemoryOnly             bool     `json:"inMemoryOnly"`
	TCPExecuted              bool     `json:"tcpExecuted"`
	DNSExecuted              bool     `json:"dnsExecuted"`
	DeadlinesExecuted        bool     `json:"deadlinesExecuted"`
	FactoryLimiterExecuted   bool     `json:"factoryLimiterExecuted"`
	LoggingExecuted          bool     `json:"loggingExecuted"`
	MetricsExecuted          bool     `json:"metricsExecuted"`
	ActualFunctionsExecuted  []string `json:"actualFunctionsExecuted"`
	SourcePinnedHarnessSteps []string `json:"sourcePinnedHarnessSteps"`
}

type metaInfoRequesterInput struct {
	Kind     string `json:"kind"`
	InfoHash string `json:"infoHash"`
	ClientID string `json:"clientId"`
	PeerID   string `json:"peerId"`
}

type metaInfoRequesterExpected struct {
	Source              *metaInfoRequesterSourceContract `json:"source"`
	ExtensionBits       *metaInfoRequesterExtensionBits  `json:"extensionBits"`
	Handshakes          []metaInfoRequesterHandshake     `json:"handshakes"`
	ExtensionHandshakes []metaInfoRequesterExHandshake   `json:"extensionHandshakes"`
	PieceRequests       []metaInfoRequesterPieceRequest  `json:"pieceRequests"`
	Messages            []metaInfoRequesterMessageRead   `json:"messages"`
	PieceReads          []metaInfoRequesterPieceRead     `json:"pieceReads"`
	Parser              *metaInfoRequesterParserResult   `json:"parser"`
	Hazards             []metaInfoRequesterHazard        `json:"hazards"`
}

type metaInfoRequesterSourceContract struct {
	MaxMetadataSize               uint64            `json:"maxMetadataSize"`
	PieceSize                     uint64            `json:"pieceSize"`
	HandshakeSize                 uint64            `json:"handshakeSize"`
	LocallyAdvertisedUTMetadataID uint64            `json:"locallyAdvertisedUtMetadataId"`
	IncomingResponseUTMetadataID  uint64            `json:"incomingResponseUtMetadataId"`
	RemoteUTMetadataMinimum       uint64            `json:"remoteUtMetadataMinimum"`
	RemoteUTMetadataMaximum       uint64            `json:"remoteUtMetadataMaximum"`
	AdvertisedExtensions          []string          `json:"advertisedExtensions"`
	SourceSHA256                  map[string]string `json:"sourceSha256"`
	DependencySHA256              map[string]string `json:"dependencySha256"`
	DependencyLines               []string          `json:"dependencyLines"`
	NormalizedASTSHA256           map[string]string `json:"normalizedAstSha256"`
	ControlledGoHazards           []string          `json:"controlledGoHazards"`
	RustHardeningAllowed          []string          `json:"rustHardeningAllowed"`
}

type metaInfoRequesterExtensionBits struct {
	ReplayDisposition      string `json:"replayDisposition"`
	DHTBit                 uint64 `json:"dhtBit"`
	LTEPBit                uint64 `json:"ltepBit"`
	DHTOnlyHex             string `json:"dhtOnlyHex"`
	LTEPOnlyHex            string `json:"ltepOnlyHex"`
	AdvertisedHex          string `json:"advertisedHex"`
	DHTEnabled             bool   `json:"dhtEnabled"`
	LTEPEnabled            bool   `json:"ltepEnabled"`
	RoundTripDisableDHTHex string `json:"roundTripDisableDhtHex"`
}

type metaInfoRequesterHandshake struct {
	ReplayDisposition      string `json:"replayDisposition"`
	Label                  string `json:"label"`
	ResponseWireHex        string `json:"responseWireHex"`
	AttemptedRequestHex    string `json:"attemptedRequestHex"`
	WriteCalls             uint64 `json:"writeCalls"`
	AttemptedBytes         uint64 `json:"attemptedBytes"`
	ReportedWrittenBytes   uint64 `json:"reportedWrittenBytes"`
	PeerID                 string `json:"peerId"`
	PeerExtensionBitsHex   string `json:"peerExtensionBitsHex"`
	Error                  string `json:"error"`
	ErrorIdentityPreserved bool   `json:"errorIdentityPreserved"`
}

type metaInfoRequesterExHandshake struct {
	ReplayDisposition             string   `json:"replayDisposition"`
	Label                         string   `json:"label"`
	ResponseWireHex               string   `json:"responseWireHex"`
	IgnoredFrameHex               []string `json:"ignoredFrameHex"`
	AttemptedAdvertisedRequestHex string   `json:"attemptedAdvertisedRequestHex"`
	WriteCalls                    uint64   `json:"writeCalls"`
	AttemptedBytes                uint64   `json:"attemptedBytes"`
	ReportedWrittenBytes          uint64   `json:"reportedWrittenBytes"`
	MetadataSizeInput             *int64   `json:"metadataSizeInput"`
	UTMetadataInput               *int64   `json:"utMetadataInput"`
	MetadataSize                  uint64   `json:"metadataSize"`
	UTMetadata                    uint64   `json:"utMetadata"`
	WriteErrorInjected            bool     `json:"writeErrorInjected"`
	WriteErrorIgnored             bool     `json:"writeErrorIgnored"`
	Error                         string   `json:"error"`
}

type metaInfoRequesterPieceRequest struct {
	ReplayDisposition string   `json:"replayDisposition"`
	Label             string   `json:"label"`
	MetadataSize      uint64   `json:"metadataSize"`
	UTMetadata        uint64   `json:"utMetadata"`
	PieceCount        uint64   `json:"pieceCount"`
	FramesHex         []string `json:"framesHex"`
	CombinedHex       string   `json:"combinedHex"`
	CombinedSHA256    string   `json:"combinedSha256"`
	Error             string   `json:"error"`
}

type metaInfoRequesterMessageRead struct {
	ReplayDisposition     string `json:"replayDisposition"`
	Label                 string `json:"label"`
	DeclaredLength        uint64 `json:"declaredLength"`
	PayloadPatternByteHex string `json:"payloadPatternByteHex"`
	PayloadPatternLength  uint64 `json:"payloadPatternLength"`
	PayloadSHA256         string `json:"payloadSha256"`
	Returned              bool   `json:"returned"`
	ReturnedIsNil         bool   `json:"returnedIsNil"`
	ReturnedLength        uint64 `json:"returnedLength"`
	ReturnedSHA256        string `json:"returnedSha256"`
	Error                 string `json:"error"`
}

type metaInfoRequesterPieceRead struct {
	ReplayDisposition string                          `json:"replayDisposition"`
	Label             string                          `json:"label"`
	MetadataSize      uint64                          `json:"metadataSize"`
	InputFrameHex     []string                        `json:"inputFrameHex"`
	InputFrameLengths []uint64                        `json:"inputFrameLengths"`
	InputPatterns     []metaInfoRequesterFramePattern `json:"inputPatterns"`
	InputByteLength   uint64                          `json:"inputByteLength"`
	InputSHA256       string                          `json:"inputSha256"`
	Returned          bool                            `json:"returned"`
	ReturnedLength    uint64                          `json:"returnedLength"`
	ReturnedSHA256    string                          `json:"returnedSha256"`
	ReturnedPrefixHex string                          `json:"returnedPrefixHex"`
	ReturnedSuffixHex string                          `json:"returnedSuffixHex"`
	Error             string                          `json:"error"`
}

type metaInfoRequesterFramePattern struct {
	Label             string `json:"label"`
	HeaderHex         string `json:"headerHex"`
	PayloadEncoding   string `json:"payloadEncoding"`
	PayloadLiteralHex string `json:"payloadLiteralHex"`
	RepeatByteHex     string `json:"repeatByteHex"`
	PayloadLength     uint64 `json:"payloadLength"`
	FrameLength       uint64 `json:"frameLength"`
	FrameSHA256       string `json:"frameSha256"`
}

type metaInfoRequesterParserResult struct {
	ReplayDisposition  string  `json:"replayDisposition"`
	RawInfoHex         string  `json:"rawInfoHex"`
	RawInfoSHA256      string  `json:"rawInfoSha256"`
	RequestedInfoHash  string  `json:"requestedInfoHash"`
	WrongRequestedHash string  `json:"wrongRequestedHash"`
	MetaVersion        uint64  `json:"metaVersion"`
	InfoHashV1         string  `json:"infoHashV1"`
	InfoHashV2         *string `json:"infoHashV2"`
	Name               string  `json:"name"`
	Length             int64   `json:"length"`
	WrongHashError     string  `json:"wrongHashError"`
}

type metaInfoRequesterHazard struct {
	ReplayDisposition        string                          `json:"replayDisposition"`
	Label                    string                          `json:"label"`
	PanicObserved            bool                            `json:"panicObserved"`
	PanicClass               string                          `json:"panicClass"`
	PanicType                string                          `json:"panicType"`
	PanicText                string                          `json:"panicText"`
	HarnessContractViolation bool                            `json:"harnessContractViolation"`
	AttemptedWireHex         string                          `json:"attemptedWireHex"`
	ReportedWrittenBytes     uint64                          `json:"reportedWrittenBytes"`
	InputPatterns            []metaInfoRequesterFramePattern `json:"inputPatterns"`
	InputByteLength          uint64                          `json:"inputByteLength"`
	InputSHA256              string                          `json:"inputSha256"`
	MetadataSize             uint64                          `json:"metadataSize"`
	Returned                 bool                            `json:"returned"`
	ReturnedLength           uint64                          `json:"returnedLength"`
	ReturnedSHA256           string                          `json:"returnedSha256"`
	DuplicateAggregateCount  uint64                          `json:"duplicateAggregateCount"`
	DistinctPieceIndexes     uint64                          `json:"distinctPieceIndexes"`
	HoleOffset               uint64                          `json:"holeOffset"`
	HoleLength               uint64                          `json:"holeLength"`
	HoleAllZero              bool                            `json:"holeAllZero"`
	RustMayReject            bool                            `json:"rustMayReject"`
}

type metaInfoRequesterScriptedReadWriter struct {
	reader             *bytes.Reader
	writes             bytes.Buffer
	writeCalls         int
	reportedWriteBytes int
	writeErr           error
	shortWriteAt       int
}

func (rw *metaInfoRequesterScriptedReadWriter) Read(p []byte) (int, error) {
	return rw.reader.Read(p)
}

func (rw *metaInfoRequesterScriptedReadWriter) Write(p []byte) (int, error) {
	rw.writeCalls++
	rw.writes.Write(p)
	if rw.writeErr != nil {
		return 0, rw.writeErr
	}
	if rw.shortWriteAt >= 0 {
		rw.reportedWriteBytes += rw.shortWriteAt
		return rw.shortWriteAt, nil
	}
	rw.reportedWriteBytes += len(p)
	return len(p), nil
}

type metaInfoRequesterASTSpec struct {
	Key      string
	Path     string
	Function string
	Receiver string
}

func TestGenerateMetaInfoRequesterParity(t *testing.T) {
	infoHash := metaInfoRequesterPatternID(0x00)
	clientID := metaInfoRequesterPatternID(0x20)
	peerID := metaInfoRequesterPatternID(0x40)
	fixtures := []metaInfoRequesterFixture{
		metaInfoRequesterSourceFixture(t),
		metaInfoRequesterHandshakeFixture(t, infoHash, clientID, peerID),
		metaInfoRequesterExtensionHandshakeFixture(t, infoHash, clientID, peerID),
		metaInfoRequesterPieceRequestFixture(t, infoHash, clientID, peerID),
		metaInfoRequesterPieceReaderFixture(t, infoHash, clientID, peerID),
		metaInfoRequesterParserFixture(t, infoHash, clientID, peerID),
		metaInfoRequesterHazardFixture(t, infoHash, clientID, peerID),
	}

	wantClassifications := [...]string{
		"SOURCE_ONLY",
		"RUNTIME_EXACT",
		"RUNTIME_EXACT_WITH_CONTROLLED_GO_HAZARD",
		"RUNTIME_EXACT",
		"RUNTIME_EXACT",
		"RUNTIME_EXACT",
		"GO_UNSAFE_CONTROLLED",
	}
	wantExecutions := [...]string{
		"SOURCE_INSPECTION_ONLY",
		"ACTUAL_EXTENSION_BIT_AND_BT_HANDSHAKE_HELPERS",
		"ACTUAL_EXTENSION_HANDSHAKE_HELPERS_AND_IGNORED_WRITE_ERROR",
		"ACTUAL_REQUEST_ALL_PIECES_AND_READ_MESSAGE_HELPERS",
		"ACTUAL_READ_ALL_PIECES_MESSAGE_FILTER_AND_ERROR_HELPERS",
		"ACTUAL_PARSE_META_INFO_BYTES_REQUESTED_HASH_VERIFICATION",
		"CONTROLLED_RECOVERY_AROUND_ACTUAL_GO_PANIC_AND_HOLE_BEHAVIORS",
	}
	if len(fixtures) != len(metaInfoRequesterFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(metaInfoRequesterFixtureIDs))
	}
	for index := range fixtures {
		fixture := fixtures[index]
		if fixture.ID != metaInfoRequesterFixtureIDs[index] {
			t.Fatalf("fixture %d ID = %q, want %q", index+1, fixture.ID, metaInfoRequesterFixtureIDs[index])
		}
		if fixture.Subsystem != metaInfoRequesterSubsystem {
			t.Fatalf("fixture %d subsystem = %q", index+1, fixture.Subsystem)
		}
		if fixture.Classification != wantClassifications[index] || fixture.Execution != wantExecutions[index] {
			t.Fatalf("fixture %d classification/execution drift", index+1)
		}
		if fixture.Oracle.TCPExecuted || fixture.Oracle.DNSExecuted || fixture.Oracle.DeadlinesExecuted ||
			fixture.Oracle.FactoryLimiterExecuted || fixture.Oracle.LoggingExecuted || fixture.Oracle.MetricsExecuted {
			t.Fatalf("fixture %d unexpectedly claims external/factory execution", index+1)
		}
		if fixture.Oracle.ActualFunctionsExecuted == nil || fixture.Oracle.SourcePinnedHarnessSteps == nil ||
			fixture.Nonclaims == nil || fixture.Expected.Handshakes == nil ||
			fixture.Expected.ExtensionHandshakes == nil || fixture.Expected.PieceRequests == nil ||
			fixture.Expected.Messages == nil || fixture.Expected.PieceReads == nil || fixture.Expected.Hazards == nil {
			t.Fatalf("fixture %d has a nil required explicit array", index+1)
		}
	}
	metaInfoRequesterReconcile(t, fixtures)
}

func metaInfoRequesterBaseFixture(
	id string,
	classification string,
	execution string,
	kind string,
	infoHash protocol.ID,
	clientID protocol.ID,
	peerID protocol.ID,
	functions []string,
) metaInfoRequesterFixture {
	return metaInfoRequesterFixture{
		ID: id, Subsystem: metaInfoRequesterSubsystem, Classification: classification, Execution: execution,
		Oracle: metaInfoRequesterOracle{
			Composition:              "actual_package_private_helpers_with_deterministic_in_memory_readers_and_writers",
			Determinism:              "exact_wire_bytes_errors_source_dependency_and_normalized_AST_SHA256",
			InMemoryOnly:             true,
			ActualFunctionsExecuted:  functions,
			SourcePinnedHarnessSteps: []string{},
		},
		Input: metaInfoRequesterInput{
			Kind: kind, InfoHash: infoHash.String(), ClientID: clientID.String(), PeerID: peerID.String(),
		},
		Expected: metaInfoRequesterExpected{
			Handshakes: []metaInfoRequesterHandshake{}, ExtensionHandshakes: []metaInfoRequesterExHandshake{},
			PieceRequests: []metaInfoRequesterPieceRequest{}, Messages: []metaInfoRequesterMessageRead{},
			PieceReads: []metaInfoRequesterPieceRead{}, Hazards: []metaInfoRequesterHazard{},
		},
		Nonclaims: metaInfoRequesterNonclaims(),
	}
}

func metaInfoRequesterNonclaims() []string {
	return []string{
		"no_TCP_connect_or_socket_IO",
		"no_DNS_resolution",
		"no_context_or_socket_deadlines",
		"no_requester_factory_or_limiter_execution",
		"no_logger_or_metrics_execution",
		"no_live_network_or_remote_peer",
		"no_Request_or_connect_end_to_end_execution",
		"no_transport_fragmentation_backpressure_or_concurrency_claim",
		"out_of_order_success_is_proven_only_for_full_sized_pieces_not_an_early_short_final_piece",
		"direct_requestAllPieces_helper_boundary_proves_outbound_ut_metadata_ID_1_and_254_only",
		"no_integrated_runtime_proof_that_exHandshake_peer_advertised_ut_metadata_ID_is_forwarded_to_requestAllPieces",
		"incoming_response_filtering_is_runtime_proven_only_for_locally_advertised_ut_metadata_ID_1",
		"no_v2_or_hybrid_parse_identity_runtime_row",
		"duplicate_or_other_piece_reader_outputs_are_not_post_assembly_hash_verified_or_accepted_by_Request",
		"no_Rust_requester_parity_or_production_wiring_claim",
	}
}

func metaInfoRequesterSourceFixture(t *testing.T) metaInfoRequesterFixture {
	t.Helper()
	fixture := metaInfoRequesterBaseFixture(
		metaInfoRequesterFixtureIDs[0], "SOURCE_ONLY", "SOURCE_INSPECTION_ONLY", "source_contract",
		protocol.ID{}, protocol.ID{}, protocol.ID{}, []string{},
	)
	fixture.Oracle.Composition = "production_requester_helper_source_and_dependency_freshness_only"
	fixture.Oracle.InMemoryOnly = false
	fixture.Oracle.SourcePinnedHarnessSteps = []string{
		"parse_and_format_exact_named_production_AST_functions",
		"hash_full_relevant_source_and_dependency_files",
		"extract_exact_anacrolix_torrent_dependency_lines",
	}
	fixture.Expected.Source = &metaInfoRequesterSourceContract{
		MaxMetadataSize: uint64(maxMetadataSize), PieceSize: metaInfoRequesterPieceSize, HandshakeSize: 68,
		LocallyAdvertisedUTMetadataID: 1,
		IncomingResponseUTMetadataID:  1,
		RemoteUTMetadataMinimum:       1,
		RemoteUTMetadataMaximum:       254,
		AdvertisedExtensions:          []string{"DHT", "LTEP"},
		SourceSHA256: metaInfoRequesterFileDigests(t, []string{
			"internal/protocol/id.go",
			"internal/protocol/infohash_v2.go",
			"internal/protocol/metainfo/metainfo.go",
			"internal/protocol/metainfo/parse.go",
			"internal/protocol/metainfo/metainforequester/requester.go",
		}),
		DependencySHA256: metaInfoRequesterFileDigests(t, []string{"go.mod", "go.sum"}),
		DependencyLines: []string{
			metaInfoRequesterDependencyLine(t, "go.mod", "github.com/anacrolix/torrent "),
			metaInfoRequesterDependencyLine(t, "go.sum", "github.com/anacrolix/torrent v1.58.0 h1:"),
			metaInfoRequesterDependencyLine(t, "go.sum", "github.com/anacrolix/torrent v1.58.0/go.mod h1:"),
		},
		NormalizedASTSHA256: metaInfoRequesterNormalizedASTDigests(t),
		ControlledGoHazards: []string{
			"extension_handshake_write_error_is_ignored_due_to_named_error_check",
			"short_bt_handshake_write_panics",
			"unchecked_metadata_piece_index_panics",
			"duplicate_piece_bytes_can_complete_aggregate_with_a_hole",
		},
		RustHardeningAllowed: []string{
			"propagate_extension_handshake_write_error",
			"complete_partial_writes_without_panicking_and_type_actual_write_failures",
			"validate_piece_index_and_piece_coverage",
			"track_unique_piece_completion_instead_of_aggregate_bytes",
		},
	}
	return fixture
}

func metaInfoRequesterHandshakeFixture(
	t *testing.T,
	infoHash protocol.ID,
	clientID protocol.ID,
	peerID protocol.ID,
) metaInfoRequesterFixture {
	t.Helper()
	fixture := metaInfoRequesterBaseFixture(
		metaInfoRequesterFixtureIDs[1], "RUNTIME_EXACT", "ACTUAL_EXTENSION_BIT_AND_BT_HANDSHAKE_HELPERS",
		"bt_handshake_and_extension_bits", infoHash, clientID, peerID,
		[]string{"NewPeerExtensionBits", "PeerExtensionBits.WithBit", "PeerExtensionBits.GetBit", "btHandshake"},
	)
	dhtOnly := NewPeerExtensionBits(ExtensionBitDht)
	ltepOnly := NewPeerExtensionBits(ExtensionBitLtep)
	advertised := NewPeerExtensionBits(ExtensionBitDht, ExtensionBitLtep)
	withoutDHT := advertised.WithBit(ExtensionBitDht, false)
	fixture.Expected.ExtensionBits = &metaInfoRequesterExtensionBits{
		ReplayDisposition: "MUST_MATCH",
		DHTBit:            uint64(ExtensionBitDht), LTEPBit: uint64(ExtensionBitLtep),
		DHTOnlyHex: hex.EncodeToString(dhtOnly[:]), LTEPOnlyHex: hex.EncodeToString(ltepOnly[:]),
		AdvertisedHex: hex.EncodeToString(advertised[:]),
		DHTEnabled:    advertised.GetBit(ExtensionBitDht), LTEPEnabled: advertised.GetBit(ExtensionBitLtep),
		RoundTripDisableDHTHex: hex.EncodeToString(withoutDHT[:]),
	}
	peerBits := NewPeerExtensionBits(ExtensionBitDht, ExtensionBitLtep, ExtensionBitV2)
	validResponse := metaInfoRequesterHandshakeWire(peerBits, infoHash, peerID)
	fixture.Expected.Handshakes = append(fixture.Expected.Handshakes,
		metaInfoRequesterRunHandshake("valid_exact_68_bytes", infoHash, clientID, validResponse, nil),
	)

	invalidProtocol := append([]byte(nil), validResponse...)
	invalidProtocol[0] = 0
	fixture.Expected.Handshakes = append(fixture.Expected.Handshakes,
		metaInfoRequesterRunHandshake("invalid_protocol", infoHash, clientID, invalidProtocol, nil),
	)
	noLTEP := metaInfoRequesterHandshakeWire(NewPeerExtensionBits(ExtensionBitDht), infoHash, peerID)
	fixture.Expected.Handshakes = append(fixture.Expected.Handshakes,
		metaInfoRequesterRunHandshake("peer_without_ltep", infoHash, clientID, noLTEP, nil),
	)
	mismatchedHash := metaInfoRequesterPatternID(0x60)
	fixture.Expected.Handshakes = append(fixture.Expected.Handshakes,
		metaInfoRequesterRunHandshake(
			"infohash_mismatch", infoHash, clientID,
			metaInfoRequesterHandshakeWire(peerBits, mismatchedHash, peerID), nil,
		),
		metaInfoRequesterRunHandshake("short_response", infoHash, clientID, validResponse[:67], nil),
	)
	writeSentinel := errors.New("handshake write sentinel")
	writeFailure := metaInfoRequesterRunHandshake("write_error", infoHash, clientID, validResponse, writeSentinel)
	fixture.Expected.Handshakes = append(fixture.Expected.Handshakes, writeFailure)

	for _, result := range fixture.Expected.Handshakes {
		if decoded, err := hex.DecodeString(result.AttemptedRequestHex); err != nil || len(decoded) != 68 {
			t.Fatalf("%s handshake request is not exact 68-byte hex", result.Label)
		}
	}
	return fixture
}

func metaInfoRequesterRunHandshake(
	label string,
	infoHash protocol.ID,
	clientID protocol.ID,
	response []byte,
	writeErr error,
) metaInfoRequesterHandshake {
	rw := &metaInfoRequesterScriptedReadWriter{
		reader: bytes.NewReader(response), writeErr: writeErr, shortWriteAt: -1,
	}
	handshake, err := btHandshake(rw, infoHash, clientID)
	result := metaInfoRequesterHandshake{
		ReplayDisposition: "MUST_MATCH", Label: label,
		ResponseWireHex: hex.EncodeToString(response), AttemptedRequestHex: hex.EncodeToString(rw.writes.Bytes()),
		WriteCalls: uint64(rw.writeCalls), AttemptedBytes: uint64(rw.writes.Len()),
		ReportedWrittenBytes: uint64(rw.reportedWriteBytes),
	}
	if err != nil {
		result.Error = err.Error()
		result.ErrorIdentityPreserved = writeErr != nil && errors.Is(err, writeErr)
		return result
	}
	result.PeerID = handshake.PeerID.String()
	result.PeerExtensionBitsHex = hex.EncodeToString(handshake.PeerExtensionBits[:])
	return result
}

func metaInfoRequesterExtensionHandshakeFixture(
	t *testing.T,
	infoHash protocol.ID,
	clientID protocol.ID,
	peerID protocol.ID,
) metaInfoRequesterFixture {
	t.Helper()
	fixture := metaInfoRequesterBaseFixture(
		metaInfoRequesterFixtureIDs[2], "RUNTIME_EXACT_WITH_CONTROLLED_GO_HAZARD",
		"ACTUAL_EXTENSION_HANDSHAKE_HELPERS_AND_IGNORED_WRITE_ERROR", "extension_handshake_boundaries",
		infoHash, clientID, peerID, []string{"exHandshake", "readExMessage", "readMessage"},
	)
	keepalive := metaInfoRequesterMessageWire(nil)
	choke := metaInfoRequesterMessageWire([]byte{byte(peer_protocol.Choke)})
	fixture.Expected.ExtensionHandshakes = append(fixture.Expected.ExtensionHandshakes,
		metaInfoRequesterRunExHandshake("minimum_values_with_ignored_nonextension_frames", 1, 1, [][]byte{keepalive, choke}, nil),
		metaInfoRequesterRunExHandshake("maximum_accepted_values", maxMetadataSize-1, 254, nil, nil),
		metaInfoRequesterRunExHandshake("zero_metadata_size", 0, 1, nil, nil),
		metaInfoRequesterRunExHandshake("maximum_metadata_size_is_exclusive", maxMetadataSize, 1, nil, nil),
		metaInfoRequesterRunExHandshake("zero_ut_metadata", 1, 0, nil, nil),
		metaInfoRequesterRunExHandshake("ut_metadata_255_is_exclusive", 1, 255, nil, nil),
	)
	writeSentinel := errors.New("extension handshake write sentinel")
	ignoredWrite := metaInfoRequesterRunExHandshake("write_error_is_ignored", 1, 1, nil, writeSentinel)
	ignoredWrite.ReplayDisposition = "GO_HAZARD_RUST_HARDENING"
	ignoredWrite.WriteErrorIgnored = ignoredWrite.Error == "" && ignoredWrite.MetadataSize == 1 && ignoredWrite.UTMetadata == 1
	fixture.Expected.ExtensionHandshakes = append(fixture.Expected.ExtensionHandshakes, ignoredWrite)

	notHandshake := metaInfoRequesterMessageWire([]byte{byte(peer_protocol.Extended), 1})
	rw := &metaInfoRequesterScriptedReadWriter{reader: bytes.NewReader(notHandshake), shortWriteAt: -1}
	_, _, err := exHandshake(rw)
	fixture.Expected.ExtensionHandshakes = append(fixture.Expected.ExtensionHandshakes, metaInfoRequesterExHandshake{
		ReplayDisposition: "MUST_MATCH", Label: "first_extension_message_not_handshake",
		ResponseWireHex: hex.EncodeToString(notHandshake), IgnoredFrameHex: []string{},
		AttemptedAdvertisedRequestHex: hex.EncodeToString(rw.writes.Bytes()), WriteCalls: uint64(rw.writeCalls),
		AttemptedBytes: uint64(rw.writes.Len()), ReportedWrittenBytes: uint64(rw.reportedWriteBytes),
		MetadataSizeInput: nil, UTMetadataInput: nil, Error: metaInfoRequesterErrorText(err),
	})
	return fixture
}

func metaInfoRequesterRunExHandshake(
	label string,
	metadataSize int,
	utMetadata int,
	ignoredFrames [][]byte,
	writeErr error,
) metaInfoRequesterExHandshake {
	response := make([]byte, 0)
	ignoredHex := make([]string, 0, len(ignoredFrames))
	for _, frame := range ignoredFrames {
		response = append(response, frame...)
		ignoredHex = append(ignoredHex, hex.EncodeToString(frame))
	}
	response = append(response, metaInfoRequesterExtensionHandshakeWire(metadataSize, utMetadata)...)
	rw := &metaInfoRequesterScriptedReadWriter{
		reader: bytes.NewReader(response), writeErr: writeErr, shortWriteAt: -1,
	}
	actualSize, actualUTMetadata, err := exHandshake(rw)
	return metaInfoRequesterExHandshake{
		ReplayDisposition: "MUST_MATCH", Label: label,
		ResponseWireHex: hex.EncodeToString(response), IgnoredFrameHex: ignoredHex,
		AttemptedAdvertisedRequestHex: hex.EncodeToString(rw.writes.Bytes()), WriteCalls: uint64(rw.writeCalls),
		AttemptedBytes: uint64(rw.writes.Len()), ReportedWrittenBytes: uint64(rw.reportedWriteBytes),
		MetadataSizeInput: metaInfoRequesterInt64Pointer(int64(metadataSize)),
		UTMetadataInput:   metaInfoRequesterInt64Pointer(int64(utMetadata)),
		MetadataSize:      uint64(actualSize), UTMetadata: uint64(actualUTMetadata),
		WriteErrorInjected: writeErr != nil, Error: metaInfoRequesterErrorText(err),
	}
}

func metaInfoRequesterPieceRequestFixture(
	t *testing.T,
	infoHash protocol.ID,
	clientID protocol.ID,
	peerID protocol.ID,
) metaInfoRequesterFixture {
	t.Helper()
	fixture := metaInfoRequesterBaseFixture(
		metaInfoRequesterFixtureIDs[3], "RUNTIME_EXACT",
		"ACTUAL_REQUEST_ALL_PIECES_AND_READ_MESSAGE_HELPERS", "piece_request_and_message_boundaries",
		infoHash, clientID, peerID, []string{"requestAllPieces", "uintToBigEndian4", "readMessage"},
	)
	requestCases := []struct {
		metadataSize uint
		utMetadata   uint8
	}{
		{metadataSize: 1, utMetadata: 1},
		{metadataSize: metaInfoRequesterPieceSize, utMetadata: 1},
		{metadataSize: metaInfoRequesterPieceSize + 1, utMetadata: 254},
	}
	for _, requestCase := range requestCases {
		var wire bytes.Buffer
		err := requestAllPieces(&wire, requestCase.metadataSize, requestCase.utMetadata)
		frames, splitErr := metaInfoRequesterSplitWireMessages(wire.Bytes())
		if splitErr != nil {
			t.Fatal(splitErr)
		}
		frameHex := make([]string, 0, len(frames))
		for _, frame := range frames {
			frameHex = append(frameHex, hex.EncodeToString(frame))
		}
		fixture.Expected.PieceRequests = append(fixture.Expected.PieceRequests, metaInfoRequesterPieceRequest{
			ReplayDisposition: "MUST_MATCH",
			Label:             fmt.Sprintf("metadata_size_%d_ut_metadata_%d", requestCase.metadataSize, requestCase.utMetadata),
			MetadataSize:      uint64(requestCase.metadataSize), UTMetadata: uint64(requestCase.utMetadata),
			PieceCount: uint64(len(frames)),
			FramesHex:  frameHex, CombinedHex: hex.EncodeToString(wire.Bytes()),
			CombinedSHA256: metaInfoRequesterSHA256(wire.Bytes()), Error: metaInfoRequesterErrorText(err),
		})
	}

	maxPayload := make([]byte, maxMetadataSize)
	maxWire := metaInfoRequesterMessageWire(maxPayload)
	maxRead, maxErr := readMessage(bytes.NewReader(maxWire))
	fixture.Expected.Messages = append(fixture.Expected.Messages, metaInfoRequesterMessageRead{
		ReplayDisposition: "MUST_MATCH", Label: "maximum_length_accepted",
		DeclaredLength: maxMetadataSize, PayloadSHA256: metaInfoRequesterSHA256(maxPayload),
		PayloadPatternByteHex: "00", PayloadPatternLength: maxMetadataSize,
		Returned: maxErr == nil, ReturnedIsNil: maxRead == nil,
		ReturnedLength: uint64(len(maxRead)), ReturnedSHA256: metaInfoRequesterSHA256(maxRead),
		Error: metaInfoRequesterErrorText(maxErr),
	})
	tooLongPrefix := uintToBigEndian4(maxMetadataSize + 1)
	tooLongRead, tooLongErr := readMessage(bytes.NewReader(tooLongPrefix))
	fixture.Expected.Messages = append(fixture.Expected.Messages, metaInfoRequesterMessageRead{
		ReplayDisposition: "MUST_MATCH", Label: "maximum_length_plus_one_rejected_before_payload_read",
		DeclaredLength: maxMetadataSize + 1, PayloadSHA256: "",
		PayloadPatternByteHex: "", PayloadPatternLength: 0,
		Returned: tooLongErr == nil, ReturnedIsNil: tooLongRead == nil,
		ReturnedLength: uint64(len(tooLongRead)), ReturnedSHA256: metaInfoRequesterSHA256(tooLongRead),
		Error: metaInfoRequesterErrorText(tooLongErr),
	})
	return fixture
}

func metaInfoRequesterSplitWireMessages(wire []byte) ([][]byte, error) {
	frames := make([][]byte, 0)
	for len(wire) != 0 {
		if len(wire) < 4 {
			return nil, errors.New("truncated length prefix")
		}
		length := int(binary.BigEndian.Uint32(wire[:4]))
		frameLength := 4 + length
		if frameLength > len(wire) {
			return nil, errors.New("truncated message")
		}
		frames = append(frames, append([]byte(nil), wire[:frameLength]...))
		wire = wire[frameLength:]
	}
	return frames, nil
}

func metaInfoRequesterPieceReaderFixture(
	t *testing.T,
	infoHash protocol.ID,
	clientID protocol.ID,
	peerID protocol.ID,
) metaInfoRequesterFixture {
	t.Helper()
	fixture := metaInfoRequesterBaseFixture(
		metaInfoRequesterFixtureIDs[4], "RUNTIME_EXACT",
		"ACTUAL_READ_ALL_PIECES_MESSAGE_FILTER_AND_ERROR_HELPERS", "piece_reader_matrix",
		infoHash, clientID, peerID,
		[]string{"readAllPieces", "readUmMessage", "readExMessage", "readMessage"},
	)
	keepalive := metaInfoRequesterMessageWire(nil)
	choke := metaInfoRequesterMessageWire([]byte{byte(peer_protocol.Choke)})
	irrelevantExtension := metaInfoRequesterMessageWire([]byte{byte(peer_protocol.Extended), 2, 0x99})
	metadataFrame := metaInfoRequesterUTMetadataWire(1, 0, []byte("meta"))
	fixture.Expected.PieceReads = append(fixture.Expected.PieceReads, metaInfoRequesterRunPieceRead(
		"irrelevant_frames_are_ignored", 4,
		[][]byte{keepalive, choke, irrelevantExtension, metadataFrame},
		[]metaInfoRequesterFramePattern{
			metaInfoRequesterNoPayloadPattern("keepalive", keepalive),
			metaInfoRequesterNoPayloadPattern("choke", choke),
			metaInfoRequesterNoPayloadPattern("extension_id_2", irrelevantExtension),
			metaInfoRequesterLiteralPayloadPattern("ut_metadata_data_piece_0", metadataFrame, []byte("meta")),
		}, "MUST_MATCH",
	))

	pieceA := bytes.Repeat([]byte{0xa1}, metaInfoRequesterPieceSize)
	pieceB := bytes.Repeat([]byte{0xb2}, metaInfoRequesterPieceSize)
	outOfOrderPiece1 := metaInfoRequesterUTMetadataWire(1, 1, pieceB)
	outOfOrderPiece0 := metaInfoRequesterUTMetadataWire(1, 0, pieceA)
	fixture.Expected.PieceReads = append(fixture.Expected.PieceReads, metaInfoRequesterRunPieceRead(
		"out_of_order_complete", 2*metaInfoRequesterPieceSize,
		[][]byte{outOfOrderPiece1, outOfOrderPiece0},
		[]metaInfoRequesterFramePattern{
			metaInfoRequesterRepeatPayloadPattern("piece_1", outOfOrderPiece1, 0xb2, metaInfoRequesterPieceSize),
			metaInfoRequesterRepeatPayloadPattern("piece_0", outOfOrderPiece0, 0xa1, metaInfoRequesterPieceSize),
		}, "MUST_MATCH",
	))
	rejectFrame := metaInfoRequesterUTMetadataWire(2, 0, nil)
	fixture.Expected.PieceReads = append(fixture.Expected.PieceReads, metaInfoRequesterRunPieceRead(
		"remote_reject", 1, [][]byte{rejectFrame},
		[]metaInfoRequesterFramePattern{metaInfoRequesterNoPayloadPattern("reject_piece_0", rejectFrame)}, "MUST_MATCH",
	))
	oversized := bytes.Repeat([]byte{0xd4}, metaInfoRequesterPieceSize+1)
	oversizedFrame := metaInfoRequesterUTMetadataWire(1, 0, oversized)
	fixture.Expected.PieceReads = append(fixture.Expected.PieceReads, metaInfoRequesterRunPieceRead(
		"oversized_piece", metaInfoRequesterPieceSize+1,
		[][]byte{oversizedFrame},
		[]metaInfoRequesterFramePattern{
			metaInfoRequesterRepeatPayloadPattern("piece_0", oversizedFrame, 0xd4, metaInfoRequesterPieceSize+1),
		}, "MUST_MATCH",
	))
	shortFrame := metaInfoRequesterUTMetadataWire(1, 0, []byte{0xe5})
	fixture.Expected.PieceReads = append(fixture.Expected.PieceReads, metaInfoRequesterRunPieceRead(
		"short_incomplete_piece", metaInfoRequesterPieceSize+1,
		[][]byte{shortFrame},
		[]metaInfoRequesterFramePattern{
			metaInfoRequesterLiteralPayloadPattern("piece_0", shortFrame, []byte{0xe5}),
		}, "MUST_MATCH",
	))
	overflowPiece0 := metaInfoRequesterUTMetadataWire(1, 0, pieceA)
	overflowPiece1 := metaInfoRequesterUTMetadataWire(1, 1, pieceB)
	fixture.Expected.PieceReads = append(fixture.Expected.PieceReads, metaInfoRequesterRunPieceRead(
		"aggregate_size_overflow", metaInfoRequesterPieceSize+1,
		[][]byte{overflowPiece0, overflowPiece1},
		[]metaInfoRequesterFramePattern{
			metaInfoRequesterRepeatPayloadPattern("piece_0", overflowPiece0, 0xa1, metaInfoRequesterPieceSize),
			metaInfoRequesterRepeatPayloadPattern("piece_1", overflowPiece1, 0xb2, metaInfoRequesterPieceSize),
		}, "MUST_MATCH",
	))
	return fixture
}

func metaInfoRequesterRunPieceRead(
	label string,
	metadataSize uint,
	frames [][]byte,
	patterns []metaInfoRequesterFramePattern,
	disposition string,
) metaInfoRequesterPieceRead {
	var input bytes.Buffer
	frameLengths := make([]uint64, 0, len(frames))
	for _, frame := range frames {
		input.Write(frame)
		frameLengths = append(frameLengths, uint64(len(frame)))
	}
	frameHex := make([]string, 0)
	if input.Len() <= 512 {
		frameHex = make([]string, 0, len(frames))
		for _, frame := range frames {
			frameHex = append(frameHex, hex.EncodeToString(frame))
		}
	}
	returned, err := readAllPieces(bytes.NewReader(input.Bytes()), metadataSize)
	result := metaInfoRequesterPieceRead{
		ReplayDisposition: disposition, Label: label, MetadataSize: uint64(metadataSize),
		InputFrameHex: frameHex, InputFrameLengths: frameLengths,
		InputPatterns:   append([]metaInfoRequesterFramePattern(nil), patterns...),
		InputByteLength: uint64(input.Len()), InputSHA256: metaInfoRequesterSHA256(input.Bytes()),
		Error: metaInfoRequesterErrorText(err),
	}
	if err == nil {
		result.Returned = true
		result.ReturnedLength = uint64(len(returned))
		result.ReturnedSHA256 = metaInfoRequesterSHA256(returned)
		prefixLength := min(len(returned), 16)
		suffixStart := max(len(returned)-16, 0)
		result.ReturnedPrefixHex = hex.EncodeToString(returned[:prefixLength])
		result.ReturnedSuffixHex = hex.EncodeToString(returned[suffixStart:])
	}
	return result
}

func metaInfoRequesterNoPayloadPattern(label string, frame []byte) metaInfoRequesterFramePattern {
	return metaInfoRequesterFramePattern{
		Label: label, HeaderHex: hex.EncodeToString(frame), PayloadEncoding: "none",
		FrameLength: uint64(len(frame)), FrameSHA256: metaInfoRequesterSHA256(frame),
	}
}

func metaInfoRequesterLiteralPayloadPattern(
	label string,
	frame []byte,
	payload []byte,
) metaInfoRequesterFramePattern {
	headerLength := len(frame) - len(payload)
	if headerLength < 0 {
		panic("payload is longer than frame")
	}
	return metaInfoRequesterFramePattern{
		Label: label, HeaderHex: hex.EncodeToString(frame[:headerLength]), PayloadEncoding: "literal_hex",
		PayloadLiteralHex: hex.EncodeToString(payload), PayloadLength: uint64(len(payload)),
		FrameLength: uint64(len(frame)), FrameSHA256: metaInfoRequesterSHA256(frame),
	}
}

func metaInfoRequesterRepeatPayloadPattern(
	label string,
	frame []byte,
	repeatByte byte,
	payloadLength int,
) metaInfoRequesterFramePattern {
	headerLength := len(frame) - payloadLength
	if headerLength < 0 {
		panic("payload is longer than frame")
	}
	return metaInfoRequesterFramePattern{
		Label: label, HeaderHex: hex.EncodeToString(frame[:headerLength]), PayloadEncoding: "repeat_byte",
		RepeatByteHex: fmt.Sprintf("%02x", repeatByte), PayloadLength: uint64(payloadLength),
		FrameLength: uint64(len(frame)), FrameSHA256: metaInfoRequesterSHA256(frame),
	}
}

func metaInfoRequesterParserFixture(
	t *testing.T,
	_ protocol.ID,
	clientID protocol.ID,
	peerID protocol.ID,
) metaInfoRequesterFixture {
	t.Helper()
	rawInfo := []byte("d6:lengthi1e4:name1:x12:piece lengthi16384e6:pieces20:")
	rawInfo = append(rawInfo, bytes.Repeat([]byte{0x70}, 20)...)
	rawInfo = append(rawInfo, 'e')
	v1 := protocol.ID(sha1.Sum(rawInfo))
	fixture := metaInfoRequesterBaseFixture(
		metaInfoRequesterFixtureIDs[5], "RUNTIME_EXACT",
		"ACTUAL_PARSE_META_INFO_BYTES_REQUESTED_HASH_VERIFICATION", "requested_hash_parse_identity",
		v1, clientID, peerID, []string{"metainfo.ParseMetaInfoBytes"},
	)
	parsed, err := metainfo.ParseMetaInfoBytes(v1, rawInfo)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.InfoHashV1 == nil {
		t.Fatal("deterministic v1 info did not produce v1 identity")
	}
	wrong := v1
	wrong[0] ^= 0xff
	_, wrongErr := metainfo.ParseMetaInfoBytes(wrong, rawInfo)
	var infoHashV2 *string
	if parsed.InfoHashV2 != nil {
		value := parsed.InfoHashV2.String()
		infoHashV2 = &value
	}
	fixture.Expected.Parser = &metaInfoRequesterParserResult{
		ReplayDisposition: "MUST_MATCH", RawInfoHex: hex.EncodeToString(rawInfo),
		RawInfoSHA256: metaInfoRequesterSHA256(rawInfo), RequestedInfoHash: v1.String(),
		WrongRequestedHash: wrong.String(), MetaVersion: uint64(parsed.MetaVersion),
		InfoHashV1: parsed.InfoHashV1.String(), InfoHashV2: infoHashV2,
		Name: parsed.Info.Name, Length: parsed.Info.Length, WrongHashError: metaInfoRequesterErrorText(wrongErr),
	}
	return fixture
}

func metaInfoRequesterHazardFixture(
	t *testing.T,
	infoHash protocol.ID,
	clientID protocol.ID,
	peerID protocol.ID,
) metaInfoRequesterFixture {
	t.Helper()
	fixture := metaInfoRequesterBaseFixture(
		metaInfoRequesterFixtureIDs[6], "GO_UNSAFE_CONTROLLED",
		"CONTROLLED_RECOVERY_AROUND_ACTUAL_GO_PANIC_AND_HOLE_BEHAVIORS", "controlled_go_hazards",
		infoHash, clientID, peerID,
		[]string{"btHandshake", "readAllPieces", "readUmMessage", "readExMessage", "readMessage"},
	)
	validResponse := metaInfoRequesterHandshakeWire(
		NewPeerExtensionBits(ExtensionBitDht, ExtensionBitLtep), infoHash, peerID,
	)
	shortRW := &metaInfoRequesterScriptedReadWriter{
		reader: bytes.NewReader(validResponse), shortWriteAt: 67,
	}
	shortPanic, shortPanicked := metaInfoRequesterCapturePanic(func() {
		_, _ = btHandshake(shortRW, infoHash, clientID)
	})
	fixture.Expected.Hazards = append(fixture.Expected.Hazards, metaInfoRequesterHazard{
		ReplayDisposition: "GO_HAZARD_RUST_HARDENING", Label: "short_handshake_write_panics",
		PanicObserved: shortPanicked, PanicClass: "literal_panic", PanicType: fmt.Sprintf("%T", shortPanic),
		PanicText: fmt.Sprint(shortPanic), HarnessContractViolation: true,
		AttemptedWireHex:     hex.EncodeToString(shortRW.writes.Bytes()),
		ReportedWrittenBytes: uint64(shortRW.reportedWriteBytes), InputPatterns: []metaInfoRequesterFramePattern{},
		RustMayReject: true,
	})

	badIndexWire := metaInfoRequesterUTMetadataWire(1, 1, []byte{0xc3})
	badIndexPanic, badIndexPanicked := metaInfoRequesterCapturePanic(func() {
		_, _ = readAllPieces(bytes.NewReader(badIndexWire), 1)
	})
	fixture.Expected.Hazards = append(fixture.Expected.Hazards, metaInfoRequesterHazard{
		ReplayDisposition: "GO_HAZARD_RUST_HARDENING", Label: "unchecked_positive_piece_index_panics",
		PanicObserved: badIndexPanicked, PanicClass: "slice_bounds_out_of_range",
		InputPatterns: []metaInfoRequesterFramePattern{
			metaInfoRequesterLiteralPayloadPattern("piece_1", badIndexWire, []byte{0xc3}),
		},
		InputByteLength: uint64(len(badIndexWire)), InputSHA256: metaInfoRequesterSHA256(badIndexWire),
		MetadataSize:  1,
		RustMayReject: true,
	})
	if !strings.Contains(fmt.Sprint(badIndexPanic), "slice bounds out of range") {
		t.Fatalf("unexpected piece-index panic: %T %v", badIndexPanic, badIndexPanic)
	}

	pieceA := bytes.Repeat([]byte{0xa1}, metaInfoRequesterPieceSize)
	pieceB := bytes.Repeat([]byte{0xb2}, metaInfoRequesterPieceSize)
	duplicateWire := append(
		metaInfoRequesterUTMetadataWire(1, 0, pieceA),
		metaInfoRequesterUTMetadataWire(1, 0, pieceB)...,
	)
	duplicateResult, duplicateErr := readAllPieces(bytes.NewReader(duplicateWire), 2*metaInfoRequesterPieceSize)
	if duplicateErr != nil {
		t.Fatal(duplicateErr)
	}
	hole := duplicateResult[metaInfoRequesterPieceSize:]
	fixture.Expected.Hazards = append(fixture.Expected.Hazards, metaInfoRequesterHazard{
		ReplayDisposition: "GO_HAZARD_RUST_HARDENING", Label: "duplicate_piece_aggregate_completion_leaves_hole",
		InputPatterns: []metaInfoRequesterFramePattern{
			metaInfoRequesterRepeatPayloadPattern(
				"piece_0_first", duplicateWire[:len(duplicateWire)/2], 0xa1, metaInfoRequesterPieceSize,
			),
			metaInfoRequesterRepeatPayloadPattern(
				"piece_0_duplicate", duplicateWire[len(duplicateWire)/2:], 0xb2, metaInfoRequesterPieceSize,
			),
		},
		InputByteLength: uint64(len(duplicateWire)), InputSHA256: metaInfoRequesterSHA256(duplicateWire),
		MetadataSize: 2 * metaInfoRequesterPieceSize,
		Returned:     true, ReturnedLength: uint64(len(duplicateResult)),
		ReturnedSHA256: metaInfoRequesterSHA256(duplicateResult), DuplicateAggregateCount: 2,
		DistinctPieceIndexes: 1, HoleOffset: metaInfoRequesterPieceSize, HoleLength: uint64(len(hole)),
		HoleAllZero: bytes.Equal(hole, make([]byte, len(hole))), RustMayReject: true,
	})
	return fixture
}

func TestMetaInfoRequesterParitySchemaRejectsUnknownFields(t *testing.T) {
	tests := []string{
		`{"unknown":1}`,
		`{"oracle":{"unknown":1}}`,
		`{"expected":{"source":{"unknown":1}}}`,
		`{"expected":{"pieceReads":[{"unknown":1}]}}`,
		`{"expected":{"pieceReads":[{"inputPatterns":[{"unknown":1}]}]}}`,
		`{"expected":{"hazards":[{"inputPatterns":[{"unknown":1}]}]}}`,
	}
	for _, encoded := range tests {
		decoder := json.NewDecoder(strings.NewReader(encoded))
		decoder.DisallowUnknownFields()
		var fixture metaInfoRequesterFixture
		if err := decoder.Decode(&fixture); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("strict decode accepted nested unknown field in %s: %v", encoded, err)
		}
	}
}

func metaInfoRequesterPatternID(start byte) (id protocol.ID) {
	for index := range id {
		id[index] = start + byte(index)
	}
	return id
}

func metaInfoRequesterHandshakeWire(
	bits PeerExtensionBits,
	infoHash protocol.ID,
	peerID protocol.ID,
) []byte {
	wire := make([]byte, 0, 68)
	wire = append(wire, peer_protocol.Protocol...)
	wire = append(wire, bits[:]...)
	wire = append(wire, infoHash[:]...)
	wire = append(wire, peerID[:]...)
	return wire
}

func metaInfoRequesterMessageWire(message []byte) []byte {
	wire := make([]byte, 4, 4+len(message))
	binary.BigEndian.PutUint32(wire, uint32(len(message)))
	return append(wire, message...)
}

func metaInfoRequesterExtensionHandshakeWire(metadataSize int, utMetadata int) []byte {
	payload, err := bencode.Marshal(rootDict{
		M: mDict{UTMetadata: utMetadata}, MetadataSize: metadataSize,
	})
	if err != nil {
		panic(err)
	}
	message := append([]byte{byte(peer_protocol.Extended), 0}, payload...)
	return metaInfoRequesterMessageWire(message)
}

func metaInfoRequesterUTMetadataWire(msgType int, piece int, payload []byte) []byte {
	header, err := bencode.Marshal(extDict{MsgType: msgType, Piece: piece})
	if err != nil {
		panic(err)
	}
	message := make([]byte, 0, 2+len(header)+len(payload))
	message = append(message, byte(peer_protocol.Extended), 1)
	message = append(message, header...)
	message = append(message, payload...)
	return metaInfoRequesterMessageWire(message)
}

func metaInfoRequesterCapturePanic(run func()) (value any, observed bool) {
	defer func() {
		value = recover()
		observed = value != nil
	}()
	run()
	return nil, false
}

func metaInfoRequesterSHA256(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func metaInfoRequesterErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func metaInfoRequesterInt64Pointer(value int64) *int64 {
	return &value
}

func metaInfoRequesterRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve metainfo requester parity source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../../../.."))
}

func metaInfoRequesterFileDigests(t *testing.T, paths []string) map[string]string {
	t.Helper()
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(filepath.Join(metaInfoRequesterRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		result[path] = metaInfoRequesterSHA256(contents)
	}
	return result
}

func metaInfoRequesterDependencyLine(t *testing.T, path string, prefix string) string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(metaInfoRequesterRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(contents), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, prefix) {
			return trimmed
		}
	}
	t.Fatalf("dependency line beginning %q not found in %s", prefix, path)
	return ""
}

func metaInfoRequesterNormalizedASTDigests(t *testing.T) map[string]string {
	t.Helper()
	specifications := []metaInfoRequesterASTSpec{
		{Key: "requester.NewPeerExtensionBits", Path: "internal/protocol/metainfo/metainforequester/requester.go", Function: "NewPeerExtensionBits"},
		{Key: "requester.PeerExtensionBits.WithBit", Path: "internal/protocol/metainfo/metainforequester/requester.go", Function: "WithBit", Receiver: "PeerExtensionBits"},
		{Key: "requester.PeerExtensionBits.GetBit", Path: "internal/protocol/metainfo/metainforequester/requester.go", Function: "GetBit", Receiver: "PeerExtensionBits"},
		{Key: "requester.btHandshake", Path: "internal/protocol/metainfo/metainforequester/requester.go", Function: "btHandshake"},
		{Key: "requester.exHandshake", Path: "internal/protocol/metainfo/metainforequester/requester.go", Function: "exHandshake"},
		{Key: "requester.requestAllPieces", Path: "internal/protocol/metainfo/metainforequester/requester.go", Function: "requestAllPieces"},
		{Key: "requester.uintToBigEndian4", Path: "internal/protocol/metainfo/metainforequester/requester.go", Function: "uintToBigEndian4"},
		{Key: "requester.readAllPieces", Path: "internal/protocol/metainfo/metainforequester/requester.go", Function: "readAllPieces"},
		{Key: "requester.readExMessage", Path: "internal/protocol/metainfo/metainforequester/requester.go", Function: "readExMessage"},
		{Key: "requester.readUmMessage", Path: "internal/protocol/metainfo/metainforequester/requester.go", Function: "readUmMessage"},
		{Key: "requester.readMessage", Path: "internal/protocol/metainfo/metainforequester/requester.go", Function: "readMessage"},
		{Key: "metainfo.ParseMetaInfoBytes", Path: "internal/protocol/metainfo/parse.go", Function: "ParseMetaInfoBytes"},
	}
	result := make(map[string]string, len(specifications))
	for _, specification := range specifications {
		fileSet := token.NewFileSet()
		path := filepath.Join(metaInfoRequesterRoot(t), specification.Path)
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var matches []*ast.FuncDecl
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Name.Name != specification.Function {
				continue
			}
			if metaInfoRequesterASTReceiver(fileSet, function) == specification.Receiver {
				matches = append(matches, function)
			}
		}
		if len(matches) != 1 {
			t.Fatalf("AST %s match count = %d, want 1", specification.Key, len(matches))
		}
		var normalized bytes.Buffer
		if err := format.Node(&normalized, fileSet, matches[0]); err != nil {
			t.Fatal(err)
		}
		result[specification.Key] = metaInfoRequesterSHA256(normalized.Bytes())
	}
	return result
}

func metaInfoRequesterASTReceiver(fileSet *token.FileSet, function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) != 1 {
		return ""
	}
	var output bytes.Buffer
	if err := format.Node(&output, fileSet, function.Recv.List[0].Type); err != nil {
		panic(err)
	}
	return output.String()
}

func metaInfoRequesterReconcile(t *testing.T, fixtures []metaInfoRequesterFixture) {
	t.Helper()
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	for _, fixture := range fixtures {
		if err := encoder.Encode(fixture); err != nil {
			t.Fatal(err)
		}
	}
	contents := encoded.Bytes()
	if len(contents) == 0 || contents[len(contents)-1] != '\n' || bytes.Contains(contents, []byte{'\r'}) {
		t.Fatal("metainfo requester fixture must be nonempty LF-only JSONL with a final LF")
	}
	metaInfoRequesterValidateStrictJSONL(t, contents, fixtures)
	actualHash := metaInfoRequesterSHA256(contents)
	path := filepath.Join(metaInfoRequesterRoot(t), "testdata/parity/dht/metainfo_requester.jsonl")
	if *updateMetaInfoRequesterParity {
		if err := os.WriteFile(path, contents, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote metainfo requester fixture with SHA-256 %s", actualHash)
		return
	}
	if metaInfoRequesterFixtureSHA256 != "" && actualHash != metaInfoRequesterFixtureSHA256 {
		t.Fatalf("fixture SHA-256 = %s, want %s", actualHash, metaInfoRequesterFixtureSHA256)
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-metainfo-requester-parity: %v", err)
	}
	if !bytes.Equal(want, contents) {
		t.Fatal("metainfo requester fixture is stale; rerun with -update-metainfo-requester-parity")
	}
}

func metaInfoRequesterValidateStrictJSONL(
	t *testing.T,
	contents []byte,
	want []metaInfoRequesterFixture,
) {
	t.Helper()
	if bytes.Count(contents, []byte{'\n'}) != len(want) {
		t.Fatalf("fixture LF count = %d, want %d", bytes.Count(contents, []byte{'\n'}), len(want))
	}
	scanner := bufio.NewScanner(bytes.NewReader(contents))
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	decoded := make([]metaInfoRequesterFixture, 0, len(want))
	for scanner.Scan() {
		decoder := json.NewDecoder(strings.NewReader(scanner.Text()))
		decoder.DisallowUnknownFields()
		var fixture metaInfoRequesterFixture
		if err := decoder.Decode(&fixture); err != nil {
			t.Fatalf("strict decode row %d: %v", len(decoded)+1, err)
		}
		var trailing json.RawMessage
		if err := decoder.Decode(&trailing); err != io.EOF {
			t.Fatalf("strict decode row %d trailing JSON: %v", len(decoded)+1, err)
		}
		metaInfoRequesterValidateFixture(t, len(decoded), fixture)
		decoded = append(decoded, fixture)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatal("strict JSONL round trip changed fixture values or nil-versus-empty semantics")
	}
}

func metaInfoRequesterValidateFixture(t *testing.T, index int, fixture metaInfoRequesterFixture) {
	t.Helper()
	if index >= len(metaInfoRequesterFixtureIDs) || fixture.ID != metaInfoRequesterFixtureIDs[index] {
		t.Fatalf("strict row %d ID drift", index+1)
	}
	metaInfoRequesterRequireHex(t, fixture.Input.InfoHash, 20, "input info hash", false)
	metaInfoRequesterRequireHex(t, fixture.Input.ClientID, 20, "input client ID", false)
	metaInfoRequesterRequireHex(t, fixture.Input.PeerID, 20, "input peer ID", false)
	if fixture.Expected.Handshakes == nil || fixture.Expected.ExtensionHandshakes == nil ||
		fixture.Expected.PieceRequests == nil || fixture.Expected.Messages == nil ||
		fixture.Expected.PieceReads == nil || fixture.Expected.Hazards == nil {
		t.Fatalf("row %d decoded a required array as nil", index+1)
	}

	switch index {
	case 0:
		if fixture.Expected.Source == nil || fixture.Expected.ExtensionBits != nil || fixture.Expected.Parser != nil {
			t.Fatal("source row optional branch drift")
		}
		metaInfoRequesterValidateSource(t, *fixture.Expected.Source)
	case 1:
		if fixture.Expected.Source != nil || fixture.Expected.ExtensionBits == nil || fixture.Expected.Parser != nil || len(fixture.Expected.Handshakes) == 0 {
			t.Fatal("handshake row optional branch drift")
		}
		if fixture.Expected.ExtensionBits.ReplayDisposition != "MUST_MATCH" {
			t.Fatal("extension-bit replay disposition drift")
		}
	case 2:
		if fixture.Expected.Source != nil || fixture.Expected.ExtensionBits != nil || fixture.Expected.Parser != nil || len(fixture.Expected.ExtensionHandshakes) == 0 {
			t.Fatal("extension-handshake row optional branch drift")
		}
	case 3:
		if fixture.Expected.Source != nil || fixture.Expected.ExtensionBits != nil || fixture.Expected.Parser != nil || len(fixture.Expected.PieceRequests) != 3 || len(fixture.Expected.Messages) != 2 {
			t.Fatal("piece-request/message row optional branch drift")
		}
	case 4:
		if fixture.Expected.Source != nil || fixture.Expected.ExtensionBits != nil || fixture.Expected.Parser != nil || len(fixture.Expected.PieceReads) != 6 {
			t.Fatal("piece-reader row optional branch drift")
		}
	case 5:
		if fixture.Expected.Source != nil || fixture.Expected.ExtensionBits != nil || fixture.Expected.Parser == nil {
			t.Fatal("parser row optional branch drift")
		}
	case 6:
		if fixture.Expected.Source != nil || fixture.Expected.ExtensionBits != nil || fixture.Expected.Parser != nil || len(fixture.Expected.Hazards) != 3 {
			t.Fatal("hazard row optional branch drift")
		}
	}

	advertised := hex.EncodeToString([]byte("\x00\x00\x00\x1a\x14\x00d1:md11:ut_metadatai1eee"))
	for _, handshake := range fixture.Expected.Handshakes {
		metaInfoRequesterRequireDisposition(t, handshake.ReplayDisposition, handshake.Label)
		metaInfoRequesterRequireHex(t, handshake.AttemptedRequestHex, 68, handshake.Label+" attempted request", false)
		metaInfoRequesterRequireVariableHex(t, handshake.ResponseWireHex, handshake.Label+" response wire", false)
		if handshake.PeerID != "" {
			metaInfoRequesterRequireHex(t, handshake.PeerID, 20, handshake.Label+" peer ID", false)
			metaInfoRequesterRequireHex(t, handshake.PeerExtensionBitsHex, 8, handshake.Label+" extension bits", false)
		}
	}
	for _, handshake := range fixture.Expected.ExtensionHandshakes {
		metaInfoRequesterRequireDisposition(t, handshake.ReplayDisposition, handshake.Label)
		if handshake.AttemptedAdvertisedRequestHex != advertised || handshake.AttemptedBytes != 30 {
			t.Fatalf("%s advertised extension request drift", handshake.Label)
		}
		metaInfoRequesterRequireVariableHex(t, handshake.ResponseWireHex, handshake.Label+" response wire", false)
	}
	for _, request := range fixture.Expected.PieceRequests {
		metaInfoRequesterRequireDisposition(t, request.ReplayDisposition, request.Label)
		wire, err := hex.DecodeString(request.CombinedHex)
		if err != nil || metaInfoRequesterSHA256(wire) != request.CombinedSHA256 {
			t.Fatalf("%s piece-request wire digest drift", request.Label)
		}
		frames, err := metaInfoRequesterSplitWireMessages(wire)
		if err != nil || len(frames) != int(request.PieceCount) || len(request.FramesHex) != len(frames) {
			t.Fatalf("%s piece-request frame partition drift", request.Label)
		}
		for frameIndex := range frames {
			if hex.EncodeToString(frames[frameIndex]) != request.FramesHex[frameIndex] {
				t.Fatalf("%s piece-request frame %d drift", request.Label, frameIndex)
			}
		}
	}
	for _, message := range fixture.Expected.Messages {
		metaInfoRequesterRequireDisposition(t, message.ReplayDisposition, message.Label)
		if message.PayloadPatternLength != 0 {
			pattern, err := hex.DecodeString(message.PayloadPatternByteHex)
			if err != nil || len(pattern) != 1 {
				t.Fatalf("%s payload pattern byte drift", message.Label)
			}
			payload := bytes.Repeat(pattern, int(message.PayloadPatternLength))
			if metaInfoRequesterSHA256(payload) != message.PayloadSHA256 {
				t.Fatalf("%s payload pattern digest drift", message.Label)
			}
		}
		if message.ReturnedIsNil == message.Returned {
			t.Fatalf("%s returned/nil markers are inconsistent", message.Label)
		}
	}
	for _, read := range fixture.Expected.PieceReads {
		metaInfoRequesterRequireDisposition(t, read.ReplayDisposition, read.Label)
		input := metaInfoRequesterReconstructPatterns(t, read.InputPatterns)
		if uint64(len(input)) != read.InputByteLength || metaInfoRequesterSHA256(input) != read.InputSHA256 {
			t.Fatalf("%s reconstructed input drift", read.Label)
		}
		if len(read.InputFrameLengths) != len(read.InputPatterns) {
			t.Fatalf("%s frame-length count drift", read.Label)
		}
		for patternIndex, pattern := range read.InputPatterns {
			if read.InputFrameLengths[patternIndex] != pattern.FrameLength {
				t.Fatalf("%s frame %d length drift", read.Label, patternIndex)
			}
		}
		if len(read.InputFrameHex) != 0 && len(read.InputFrameHex) != len(read.InputPatterns) {
			t.Fatalf("%s compact full-frame count drift", read.Label)
		}
		if len(read.InputFrameHex) != 0 {
			offset := 0
			for frameIndex, pattern := range read.InputPatterns {
				end := offset + int(pattern.FrameLength)
				if end > len(input) || hex.EncodeToString(input[offset:end]) != read.InputFrameHex[frameIndex] {
					t.Fatalf("%s compact full frame %d drift", read.Label, frameIndex)
				}
				offset = end
			}
		}
	}
	for _, hazard := range fixture.Expected.Hazards {
		metaInfoRequesterRequireDisposition(t, hazard.ReplayDisposition, hazard.Label)
		if hazard.ReplayDisposition != "GO_HAZARD_RUST_HARDENING" || !hazard.RustMayReject {
			t.Fatalf("%s hazard classification drift", hazard.Label)
		}
		if len(hazard.InputPatterns) != 0 {
			input := metaInfoRequesterReconstructPatterns(t, hazard.InputPatterns)
			if uint64(len(input)) != hazard.InputByteLength || metaInfoRequesterSHA256(input) != hazard.InputSHA256 {
				t.Fatalf("%s hazard reconstructed input drift", hazard.Label)
			}
		}
	}
	if fixture.Expected.Parser != nil {
		parserResult := fixture.Expected.Parser
		metaInfoRequesterRequireDisposition(t, parserResult.ReplayDisposition, "parser")
		raw, err := hex.DecodeString(parserResult.RawInfoHex)
		if err != nil || metaInfoRequesterSHA256(raw) != parserResult.RawInfoSHA256 {
			t.Fatal("parser raw-info digest drift")
		}
		requested := protocol.ID(sha1.Sum(raw))
		if requested.String() != parserResult.RequestedInfoHash ||
			fixture.Input.InfoHash != parserResult.RequestedInfoHash ||
			parserResult.InfoHashV1 != parserResult.RequestedInfoHash {
			t.Fatal("parser v1 requested identity drift")
		}
		if parserResult.InfoHashV2 != nil {
			t.Fatal("synthetic v1 parser row unexpectedly has v2 identity")
		}
	}
	metaInfoRequesterValidateExactOutcomes(t, fixture)
}

func metaInfoRequesterValidateExactOutcomes(t *testing.T, fixture metaInfoRequesterFixture) {
	t.Helper()
	switch fixture.ID {
	case "bt_handshake_and_extension_bits":
		wantErrors := []string{
			"",
			"invalid handshake response received",
			"peer does not support the extension protocol",
			"infohash mismatch",
			"failed to read all handshake bytes (67): unexpected EOF / 000102030405060708090a0b0c0d0e0f10111213",
			"handshake write sentinel",
		}
		if len(fixture.Expected.Handshakes) != len(wantErrors) {
			t.Fatal("handshake result count drift")
		}
		for index, want := range wantErrors {
			if fixture.Expected.Handshakes[index].Error != want {
				t.Fatalf("handshake %d error = %q, want %q", index, fixture.Expected.Handshakes[index].Error, want)
			}
		}
		writeFailure := fixture.Expected.Handshakes[len(fixture.Expected.Handshakes)-1]
		if writeFailure.ReportedWrittenBytes != 0 || !writeFailure.ErrorIdentityPreserved {
			t.Fatal("handshake write-error identity/accounting drift")
		}
	case "extension_handshake_boundaries":
		wantErrors := []string{
			"", "", "metadata too big or its size is less than or equal zero",
			"metadata too big or its size is less than or equal zero",
			"ut_metadata is not an uint8", "ut_metadata is not an uint8", "",
			"first extension message is not an extension handshake",
		}
		if len(fixture.Expected.ExtensionHandshakes) != len(wantErrors) {
			t.Fatal("extension-handshake result count drift")
		}
		for index, want := range wantErrors {
			if fixture.Expected.ExtensionHandshakes[index].Error != want {
				t.Fatalf("extension handshake %d error = %q, want %q", index, fixture.Expected.ExtensionHandshakes[index].Error, want)
			}
		}
		ignoredWrite := fixture.Expected.ExtensionHandshakes[6]
		if ignoredWrite.ReplayDisposition != "GO_HAZARD_RUST_HARDENING" ||
			!ignoredWrite.WriteErrorInjected || !ignoredWrite.WriteErrorIgnored || ignoredWrite.ReportedWrittenBytes != 0 ||
			ignoredWrite.MetadataSize != 1 || ignoredWrite.UTMetadata != 1 {
			t.Fatal("ignored extension-handshake write error drift")
		}
	case "piece_request_and_message_boundaries":
		wantSizes := []uint64{1, 16384, 16385}
		wantUTMetadata := []uint64{1, 1, 254}
		wantCounts := []uint64{1, 1, 2}
		for index := range wantSizes {
			request := fixture.Expected.PieceRequests[index]
			if request.MetadataSize != wantSizes[index] || request.UTMetadata != wantUTMetadata[index] ||
				request.PieceCount != wantCounts[index] || request.Error != "" {
				t.Fatalf("piece request %d boundary drift", index)
			}
		}
		maximum := fixture.Expected.Messages[0]
		if maximum.PayloadSHA256 != "e5b844cc57f57094ea4585e235f36c78c1cd222262bb89d53c94dcb4d6b3e55d" ||
			maximum.ReturnedSHA256 != maximum.PayloadSHA256 || maximum.ReturnedLength != maxMetadataSize {
			t.Fatal("maximum readMessage boundary drift")
		}
		if fixture.Expected.Messages[1].Error != "message is longer than max allowed metadata size" {
			t.Fatal("over-maximum readMessage error drift")
		}
	case "piece_reader_matrix":
		wantErrors := []string{
			"", "", "remote peer rejected sending metadataBytes", "metadataPiece > 16kiB",
			"metadataPiece < 16 kiB but incomplete", "receivedSize > metadataSize",
		}
		for index, want := range wantErrors {
			if fixture.Expected.PieceReads[index].Error != want {
				t.Fatalf("piece read %d error = %q, want %q", index, fixture.Expected.PieceReads[index].Error, want)
			}
		}
		outOfOrder := fixture.Expected.PieceReads[1]
		if !outOfOrder.Returned || outOfOrder.ReturnedSHA256 != "f64cc193d0dfac3e92a0c9431ff5287bd07a95ea4115449b8aec1660cc90e85e" {
			t.Fatal("out-of-order full-piece output drift")
		}
	case "requested_hash_parse_identity":
		parserResult := fixture.Expected.Parser
		if parserResult == nil || parserResult.RequestedInfoHash != "6f7e1d8f38dbef04cf5758be4ad8d39fb30643a5" ||
			parserResult.RawInfoSHA256 != "46f35870743e2e46b31555565d955728bc9a7b3f8c9b9141f558e4661b0cffc2" ||
			parserResult.MetaVersion != 1 || parserResult.Name != "x" || parserResult.Length != 1 ||
			parserResult.WrongHashError != "info bytes have wrong hash" {
			t.Fatal("requested-hash parser identity drift")
		}
	case "controlled_go_hazards":
		shortWrite := fixture.Expected.Hazards[0]
		if !shortWrite.PanicObserved || shortWrite.PanicText != "handshake bytes must have length 68" ||
			!shortWrite.HarnessContractViolation || shortWrite.ReportedWrittenBytes != 67 {
			t.Fatal("short-write controlled panic drift")
		}
		badIndex := fixture.Expected.Hazards[1]
		if !badIndex.PanicObserved || badIndex.PanicClass != "slice_bounds_out_of_range" ||
			badIndex.PanicType != "" || badIndex.PanicText != "" || badIndex.MetadataSize != 1 {
			t.Fatal("unchecked piece-index controlled panic drift")
		}
		duplicate := fixture.Expected.Hazards[2]
		if !duplicate.Returned || duplicate.ReturnedSHA256 != "37ea201d13607d087ccbed5457975c05a7414677cdd595112e788bb795eba240" ||
			duplicate.DuplicateAggregateCount != 2 || duplicate.DistinctPieceIndexes != 1 ||
			duplicate.HoleOffset != 16384 || duplicate.HoleLength != 16384 || !duplicate.HoleAllZero {
			t.Fatal("duplicate-piece aggregate hole drift")
		}
	}
}

func metaInfoRequesterValidateSource(t *testing.T, source metaInfoRequesterSourceContract) {
	t.Helper()
	metaInfoRequesterRequireExactKeys(t, source.SourceSHA256, []string{
		"internal/protocol/id.go",
		"internal/protocol/infohash_v2.go",
		"internal/protocol/metainfo/metainfo.go",
		"internal/protocol/metainfo/parse.go",
		"internal/protocol/metainfo/metainforequester/requester.go",
	}, "source SHA-256")
	metaInfoRequesterRequireExactKeys(t, source.DependencySHA256, []string{"go.mod", "go.sum"}, "dependency SHA-256")
	metaInfoRequesterRequireExactKeys(t, source.NormalizedASTSHA256, []string{
		"metainfo.ParseMetaInfoBytes",
		"requester.NewPeerExtensionBits",
		"requester.PeerExtensionBits.GetBit",
		"requester.PeerExtensionBits.WithBit",
		"requester.btHandshake",
		"requester.exHandshake",
		"requester.readAllPieces",
		"requester.readExMessage",
		"requester.readMessage",
		"requester.readUmMessage",
		"requester.requestAllPieces",
		"requester.uintToBigEndian4",
	}, "normalized AST SHA-256")
	for path, digest := range source.SourceSHA256 {
		metaInfoRequesterRequireHex(t, digest, 32, path+" source SHA-256", false)
	}
	for path, digest := range source.DependencySHA256 {
		metaInfoRequesterRequireHex(t, digest, 32, path+" dependency SHA-256", false)
	}
	for key, digest := range source.NormalizedASTSHA256 {
		metaInfoRequesterRequireHex(t, digest, 32, key+" normalized AST SHA-256", false)
	}
	if len(source.DependencyLines) != 3 || len(source.ControlledGoHazards) != 4 || len(source.RustHardeningAllowed) != 4 {
		t.Fatal("source dependency or hazard inventory cardinality drift")
	}
	if source.LocallyAdvertisedUTMetadataID != 1 || source.IncomingResponseUTMetadataID != 1 ||
		source.RemoteUTMetadataMinimum != 1 || source.RemoteUTMetadataMaximum != 254 {
		t.Fatal("BEP-10/BEP-9 directional extension-ID facts drift")
	}
}

func metaInfoRequesterRequireExactKeys(
	t *testing.T,
	values map[string]string,
	want []string,
	label string,
) {
	t.Helper()
	if len(values) != len(want) {
		t.Fatalf("%s key count = %d, want %d", label, len(values), len(want))
	}
	for _, key := range want {
		if _, ok := values[key]; !ok {
			t.Fatalf("%s missing key %q", label, key)
		}
	}
}

func metaInfoRequesterRequireDisposition(t *testing.T, disposition string, label string) {
	t.Helper()
	if disposition != "MUST_MATCH" && disposition != "GO_HAZARD_RUST_HARDENING" {
		t.Fatalf("%s replay disposition = %q", label, disposition)
	}
}

func metaInfoRequesterReconstructPatterns(
	t *testing.T,
	patterns []metaInfoRequesterFramePattern,
) []byte {
	t.Helper()
	var combined bytes.Buffer
	for index, pattern := range patterns {
		header, err := hex.DecodeString(pattern.HeaderHex)
		if err != nil {
			t.Fatalf("pattern %d header hex: %v", index, err)
		}
		frame := append([]byte(nil), header...)
		switch pattern.PayloadEncoding {
		case "none":
			if pattern.PayloadLength != 0 || pattern.PayloadLiteralHex != "" || pattern.RepeatByteHex != "" {
				t.Fatalf("pattern %d invalid none payload", index)
			}
		case "literal_hex":
			payload, err := hex.DecodeString(pattern.PayloadLiteralHex)
			if err != nil || uint64(len(payload)) != pattern.PayloadLength || pattern.RepeatByteHex != "" {
				t.Fatalf("pattern %d invalid literal payload", index)
			}
			frame = append(frame, payload...)
		case "repeat_byte":
			repeatByte, err := hex.DecodeString(pattern.RepeatByteHex)
			if err != nil || len(repeatByte) != 1 || pattern.PayloadLiteralHex != "" {
				t.Fatalf("pattern %d invalid repeat payload", index)
			}
			frame = append(frame, bytes.Repeat(repeatByte, int(pattern.PayloadLength))...)
		default:
			t.Fatalf("pattern %d encoding = %q", index, pattern.PayloadEncoding)
		}
		if uint64(len(frame)) != pattern.FrameLength || metaInfoRequesterSHA256(frame) != pattern.FrameSHA256 {
			t.Fatalf("pattern %d frame length/digest drift", index)
		}
		combined.Write(frame)
	}
	return combined.Bytes()
}

func metaInfoRequesterRequireHex(
	t *testing.T,
	value string,
	width int,
	label string,
	allowEmpty bool,
) {
	t.Helper()
	if value == "" && allowEmpty {
		return
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != width || value != strings.ToLower(value) {
		t.Fatalf("%s = %q, want lowercase %d-byte hex", label, value, width)
	}
}

func metaInfoRequesterRequireVariableHex(t *testing.T, value string, label string, allowEmpty bool) {
	t.Helper()
	if value == "" && allowEmpty {
		return
	}
	if value == "" || value != strings.ToLower(value) {
		t.Fatalf("%s = %q, want nonempty lowercase hex", label, value)
	}
	if _, err := hex.DecodeString(value); err != nil {
		t.Fatalf("%s invalid hex: %v", label, err)
	}
}
