package parity

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable/btree"
)

const dhtRoutingTreeSubsystem = "dht_routing_tree"

type dhtRoutingTreeInput struct {
	Origin           string                    `json:"origin"`
	K                int                       `json:"k"`
	SplittingEnabled bool                      `json:"splittingEnabled"`
	Operations       []dhtRoutingTreeOperation `json:"operations"`
}

type dhtRoutingTreeOperation struct {
	Kind  string `json:"kind"`
	ID    string `json:"id,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

type dhtRoutingTreeExpected struct {
	Bits    int                    `json:"bits"`
	Results []dhtRoutingTreeResult `json:"results"`
}

type dhtRoutingTreeResult struct {
	PutResult     string    `json:"putResult,omitempty"`
	BoolResult    *bool     `json:"boolResult,omitempty"`
	Closest       *[]string `json:"closest,omitempty"`
	Count         int       `json:"count"`
	Members       []string  `json:"members"`
	TargetPresent *bool     `json:"targetPresent,omitempty"`
}

func TestGenerateDHTRoutingTreeFixtures(t *testing.T) {
	zero := strings.Repeat("00", 20)
	patternedOrigin := "00112233445566778899aabbccddeeff10203040"
	nearIDs := []string{
		routingDistance("0000000000000000000000000000000000000001"),
		routingDistance("0000000000000000000000000000000000000002"),
		routingDistance("0000000000000000000000000000000000000003"),
		routingDistance("4000000000000000000000000000000000000000"),
		routingDistance("8000000000000000000000000000000000000000"),
		routingDistance("c000000000000000000000000000000000000000"),
	}

	originalDistances := []string{
		"0048", "004c", "004e", "004f", "0030", "0031", "0032", "0033",
		"8008", "8009", "800a", "800b", "0024", "0025", "0026", "0027",
		"0034", "0038", "a008", "a009",
	}
	originalIDs := make([]string, len(originalDistances))
	for index, prefix := range originalDistances {
		distance := prefix + strings.Repeat("00", 18)
		originalIDs[index] = routingIDAtDistance(patternedOrigin, distance)
	}

	unsplitK4 := []dhtRoutingTreeOperation{put(patternedOrigin)}
	for _, id := range originalIDs[:12] {
		unsplitK4 = append(unsplitK4, put(id))
	}
	for _, id := range originalIDs[12:] {
		unsplitK4 = append(unsplitK4, put(id))
	}
	for _, id := range originalIDs[:12] {
		unsplitK4 = append(unsplitK4, put(id))
	}
	unsplitK4 = append(unsplitK4,
		closest(patternedOrigin, 0),
		closest(originalIDs[16], 4),
		closest(originalIDs[18], 100),
	)
	for _, id := range originalIDs[:12] {
		unsplitK4 = append(unsplitK4, drop(id), drop(id))
	}
	// A drained tree must admit an identical second cycle.
	for _, id := range originalIDs[:12] {
		unsplitK4 = append(unsplitK4, put(id))
	}

	splitK4 := []dhtRoutingTreeOperation{put(patternedOrigin)}
	for _, id := range originalIDs[:16] {
		splitK4 = append(splitK4, put(id))
	}
	for _, id := range originalIDs[16:] {
		splitK4 = append(splitK4, put(id))
	}
	splitK4 = append(splitK4,
		closest(originalIDs[16], 4),
		closest(originalIDs[18], 100),
		closest(patternedOrigin, 100),
	)

	capacity80 := make([]dhtRoutingTreeOperation, 0, 85)
	capacity80IDs := make([]string, 81)
	for index := range capacity80IDs {
		bytes := make([]byte, 20)
		bytes[0] = 0x80
		bytes[18] = byte((index + 1) >> 8)
		bytes[19] = byte(index + 1)
		capacity80IDs[index] = hex.EncodeToString(bytes)
		capacity80 = append(capacity80, put(capacity80IDs[index]))
	}
	capacity80 = append(capacity80,
		drop(capacity80IDs[0]),
		put(capacity80IDs[80]),
		closest(zero, 100),
	)

	edgeIDs := []string{
		"8000000000000000000000000000000000000000",
		"4000000000000000000000000000000000000000",
		"0100000000000000000000000000000000000000",
		"0080000000000000000000000000000000000000",
		"0000000100000000000000000000000000000000",
		"0000000000000001000000000000000000000000",
		"0000000000000000000000000000000100000000",
		"0000000000000000000000000000000000000002",
		"0000000000000000000000000000000000000001",
	}
	edges := make([]dhtRoutingTreeOperation, 0, len(edgeIDs)+4)
	for _, id := range edgeIDs {
		edges = append(edges, put(id))
	}
	edges = append(edges,
		put("0000000000000000000000000000000000000003"),
		closest(zero, 160),
		closest(edgeIDs[4], 1),
	)

	forwardClosest := make([]dhtRoutingTreeOperation, 0, len(nearIDs)+3)
	reverseClosest := make([]dhtRoutingTreeOperation, 0, len(nearIDs)+3)
	for _, id := range nearIDs {
		forwardClosest = append(forwardClosest, put(id))
	}
	for index := len(nearIDs) - 1; index >= 0; index-- {
		reverseClosest = append(reverseClosest, put(nearIDs[index]))
	}
	for _, target := range []string{zero, nearIDs[1], "2000000000000000000000000000000000000000"} {
		forwardClosest = append(forwardClosest, closest(target, 80))
		reverseClosest = append(reverseClosest, closest(target, 80))
	}

	mixedDistances := []string{
		"0000000000000000000000000000000000000002",
		"0000000000000000000000000000000000000003",
		"2000000000000000000000000000000000000000",
		"4000000000000000000000000000000000000000",
		"8000000000000000000000000000000000000000",
	}
	mixedIDs := make([]string, len(mixedDistances))
	for index, distance := range mixedDistances {
		mixedIDs[index] = routingIDAtDistance(patternedOrigin, distance)
	}
	mixed := []dhtRoutingTreeOperation{
		put(mixedIDs[0]), put(mixedIDs[1]), put(mixedIDs[2]), put(mixedIDs[3]), put(mixedIDs[4]),
		has(mixedIDs[2]), has(routingIDAtDistance(patternedOrigin, "1000000000000000000000000000000000000000")),
		closest(routingIDAtDistance(patternedOrigin, "3000000000000000000000000000000000000000"), 3),
		drop(mixedIDs[0]), drop(mixedIDs[4]), drop(mixedIDs[4]),
		closest(patternedOrigin, 80), put(mixedIDs[4]), put(mixedIDs[0]), closest(patternedOrigin, 80),
	}
	goTraversalIDs := []string{
		"0000000000000000000000000000000000000001",
		"4000000000000000000000000000000000000000",
		"8000000000000000000000000000000000000000",
		"ffffffffffffffffffffffffffffffffffffffff",
		"5555555555555555555555555555555555555554",
	}
	goTraversal := make([]dhtRoutingTreeOperation, 0, len(goTraversalIDs)+1)
	for index := len(goTraversalIDs) - 1; index >= 0; index-- {
		goTraversal = append(goTraversal, put(goTraversalIDs[index]))
	}
	goTraversal = append(goTraversal, closest("5555555555555555555555555555555555555555", 80))

	scenarios := []struct {
		id    string
		input dhtRoutingTreeInput
	}{
		{"empty_and_origin", dhtRoutingTreeInput{Origin: patternedOrigin, K: 4, SplittingEnabled: true, Operations: []dhtRoutingTreeOperation{count(), has(patternedOrigin), drop(patternedOrigin), put(patternedOrigin), closest(patternedOrigin, 0), closest(patternedOrigin, 80)}}},
		{"capacity_zero", dhtRoutingTreeInput{Origin: zero, K: 0, SplittingEnabled: true, Operations: []dhtRoutingTreeOperation{put(nearIDs[0]), put(nearIDs[4]), count()}}},
		{"capacity_one_unsplit_reopens", dhtRoutingTreeInput{Origin: zero, K: 1, Operations: []dhtRoutingTreeOperation{put(nearIDs[4]), put(nearIDs[4]), put(nearIDs[5]), put(nearIDs[3]), put("6000000000000000000000000000000000000000"), drop(nearIDs[4]), put(nearIDs[5]), closest(zero, 80)}}},
		{"split_leaf_predicate_forward", dhtRoutingTreeInput{Origin: zero, K: 1, SplittingEnabled: true, Operations: []dhtRoutingTreeOperation{put(nearIDs[4]), put(nearIDs[5]), closest(zero, 80)}}},
		{"split_leaf_predicate_reverse", dhtRoutingTreeInput{Origin: zero, K: 1, SplittingEnabled: true, Operations: []dhtRoutingTreeOperation{put(nearIDs[5]), put(nearIDs[4]), closest(zero, 80)}}},
		{"original_k4_unsplit_two_cycles", dhtRoutingTreeInput{Origin: patternedOrigin, K: 4, Operations: unsplitK4}},
		{"original_k4_splitting", dhtRoutingTreeInput{Origin: patternedOrigin, K: 4, SplittingEnabled: true, Operations: splitK4}},
		{"capacity_eighty_boundary", dhtRoutingTreeInput{Origin: zero, K: 80, Operations: capacity80}},
		{"edge_buckets_and_last_bit", dhtRoutingTreeInput{Origin: zero, K: 1, Operations: edges}},
		{"closest_forward_insertion", dhtRoutingTreeInput{Origin: zero, K: 80, SplittingEnabled: true, Operations: forwardClosest}},
		{"closest_reverse_insertion", dhtRoutingTreeInput{Origin: zero, K: 80, SplittingEnabled: true, Operations: reverseClosest}},
		{"branch_compaction_and_mixed_trace", dhtRoutingTreeInput{Origin: patternedOrigin, K: 80, SplittingEnabled: true, Operations: mixed}},
		{"closest_go_sibling_traversal", dhtRoutingTreeInput{Origin: zero, K: 80, SplittingEnabled: true, Operations: goTraversal}},
	}

	fixtures := make([]Fixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixtures = append(fixtures, runDHTRoutingTreeScenario(t, scenario.id, scenario.input))
	}
	reconcileDHTFixtures(t, "routing_tree.jsonl", fixtures)
}

func runDHTRoutingTreeScenario(t *testing.T, id string, input dhtRoutingTreeInput) Fixture {
	t.Helper()
	origin := mustRoutingID(t, input.Origin)
	tree := btree.New(origin, input.K, input.SplittingEnabled)
	expected := dhtRoutingTreeExpected{Bits: tree.N(), Results: make([]dhtRoutingTreeResult, 0, len(input.Operations))}
	for _, operation := range input.Operations {
		result := dhtRoutingTreeResult{}
		var target btree.NodeID
		if operation.ID != "" {
			target = mustRoutingID(t, operation.ID)
		}
		switch operation.Kind {
		case "put":
			result.PutResult = tree.Put(target).String()
			present := tree.Has(target)
			result.TargetPresent = &present
		case "drop":
			value := tree.Drop(target)
			result.BoolResult = &value
			present := tree.Has(target)
			result.TargetPresent = &present
		case "has":
			value := tree.Has(target)
			result.BoolResult = &value
			result.TargetPresent = &value
		case "closest":
			ids := routingIDStrings(tree.Closest(target, operation.Limit))
			result.Closest = &ids
		case "count":
		default:
			t.Fatalf("%s: unknown operation %q", id, operation.Kind)
		}
		result.Count = tree.Count()
		result.Members = routingIDStrings(tree.Closest(origin, tree.Count()+1))
		expected.Results = append(expected.Results, result)
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	return Fixture{ID: id, Subsystem: dhtRoutingTreeSubsystem, Input: inputJSON, Expected: expectedJSON}
}

func routingIDStrings(ids []btree.NodeID) []string {
	result := make([]string, len(ids))
	for index, id := range ids {
		result[index] = hex.EncodeToString(id)
	}
	return result
}

func mustRoutingID(t *testing.T, value string) btree.NodeID {
	t.Helper()
	id, err := hex.DecodeString(value)
	if err != nil || len(id) != 20 {
		t.Fatalf("invalid 160-bit routing ID %q", value)
	}
	return btree.NodeID(id)
}

func routingDistance(value string) string {
	if len(value) != 40 {
		panic("routing distance must be 20 bytes")
	}
	return value
}

func routingIDAtDistance(origin, distance string) string {
	left, err := hex.DecodeString(origin)
	if err != nil || len(left) != 20 {
		panic("invalid routing origin")
	}
	right, err := hex.DecodeString(distance)
	if err != nil || len(right) != 20 {
		panic("invalid routing distance")
	}
	for index := range left {
		left[index] ^= right[index]
	}
	return hex.EncodeToString(left)
}

func put(id string) dhtRoutingTreeOperation  { return dhtRoutingTreeOperation{Kind: "put", ID: id} }
func drop(id string) dhtRoutingTreeOperation { return dhtRoutingTreeOperation{Kind: "drop", ID: id} }
func has(id string) dhtRoutingTreeOperation  { return dhtRoutingTreeOperation{Kind: "has", ID: id} }
func count() dhtRoutingTreeOperation         { return dhtRoutingTreeOperation{Kind: "count"} }
func closest(id string, limit int) dhtRoutingTreeOperation {
	return dhtRoutingTreeOperation{Kind: "closest", ID: id, Limit: limit}
}
