package ktable

import (
	"bytes"
	"encoding/json"
	"flag"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

var updateDHTNodeTableParity = flag.Bool(
	"update-dht-node-table-parity",
	false,
	"rewrite the Rust DHT node-table parity fixture",
)

type nodeTableFixture struct {
	ID        string            `json:"id"`
	Subsystem string            `json:"subsystem"`
	Input     nodeTableInput    `json:"input"`
	Expected  nodeTableExpected `json:"expected"`
}

type nodeTableInput struct {
	Origin     string               `json:"origin"`
	Operations []nodeTableOperation `json:"operations"`
}

type nodeTableOperation struct {
	Kind string            `json:"kind"`
	ID   string            `json:"id,omitempty"`
	Addr *nodeTableAddress `json:"addr,omitempty"`
}

type nodeTableAddress struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope,omitempty"`
}

type nodeTableExpected struct {
	Results []nodeTableResult `json:"results"`
}

type nodeTableResult struct {
	PutResult       string           `json:"putResult,omitempty"`
	DropResult      *bool            `json:"dropResult,omitempty"`
	Closest         *[]nodeTableNode `json:"closest,omitempty"`
	Origin          string           `json:"origin,omitempty"`
	RustUnsupported bool             `json:"rustUnsupported,omitempty"`
	Count           int              `json:"count"`
	State           []nodeTableNode  `json:"state"`
}

type nodeTableNode struct {
	ID   string           `json:"id"`
	Addr nodeTableAddress `json:"addr"`
}

func TestGenerateDHTNodeTableParity(t *testing.T) {
	zero := protocol.ID{}.String()
	origin := "00112233445566778899aabbccddeeff10203040"

	addressOps := []nodeTableOperation{
		putNode(nodeID(1), addr("192.0.2.1", 0, 0)),
		putNode(nodeID(2), addr("::ffff:192.0.2.2", 6881, 0)),
		putNode(nodeID(3), addr("fe80::1", 6881, 7)),
		putNode(nodeID(4), addr("fe80::1", 6881, 8)),
		putNode(nodeID(5), addr("198.51.100.9", 51413, 0)),
		putNode(nodeID(6), addr("198.51.100.9", 51413, 0)),
		closestNode(nodeID(1)),
		closestNode(nodeID(7)),
		putNode(nodeID(1), addr("fe80::2", 0, 9)),
		closestNode(nodeID(1)),
		dropNode(nodeID(5)),
		closestNode(nodeID(6)),
	}

	capacityOps := make([]nodeTableOperation, 0, 87)
	capacityIDs := make([]string, 81)
	for index := range capacityIDs {
		if index == 80 {
			capacityIDs[index] = "ffffffffffffffffffffffffffffffffffffffff"
		} else {
			capacityIDs[index] = nodeIDWithFirstByte(0xc0, index+1)
		}
		capacityOps = append(capacityOps, putNode(
			capacityIDs[index],
			addr("203.0.113.10", uint16(index), 0),
		))
	}
	capacityOps = append(capacityOps,
		putNode(capacityIDs[0], addr("2001:db8::80", 0, 11)),
		closestNode(capacityIDs[0]),
		dropNode(capacityIDs[0]),
		dropNode(capacityIDs[0]),
		putNode(capacityIDs[80], addr("2001:db8::81", 65535, 12)),
		closestNode(zero),
	)

	distances := []string{
		"0000000000000000000000000000000000000001",
		"0000000000000000000000000000000000000002",
		"2000000000000000000000000000000000000000",
		"4000000000000000000000000000000000000000",
		"8000000000000000000000000000000000000000",
		"ffffffffffffffffffffffffffffffffffffffff",
	}
	traversalIDs := make([]string, len(distances))
	for index, distance := range distances {
		traversalIDs[index] = idAtDistance(origin, distance)
	}
	forwardOps := make([]nodeTableOperation, 0, len(traversalIDs)+5)
	reverseOps := make([]nodeTableOperation, 0, len(traversalIDs)+5)
	for index, id := range traversalIDs {
		forwardOps = append(forwardOps, putNode(id, addr("192.0.2.20", uint16(6000+index), 0)))
	}
	for index := len(traversalIDs) - 1; index >= 0; index-- {
		reverseOps = append(reverseOps, putNode(traversalIDs[index], addr("192.0.2.20", uint16(6000+index), 0)))
	}
	queries := []nodeTableOperation{
		closestNode(traversalIDs[2]),
		closestNode(idAtDistance(origin, "3000000000000000000000000000000000000000")),
		closestNode(origin),
		closestNode(idAtDistance(origin, "5555555555555555555555555555555555555555")),
	}
	forwardOps = append(forwardOps, queries...)
	reverseOps = append(reverseOps, queries...)

	drainIDs := []string{nodeID(11), nodeID(12), nodeID(13), nodeIDWithFirstByte(0x80, 14)}
	drainOps := make([]nodeTableOperation, 0, 20)
	for index, id := range drainIDs {
		drainOps = append(drainOps, putNode(id, addr("192.0.2.30", uint16(7000+index), 0)))
	}
	for _, id := range drainIDs {
		drainOps = append(drainOps, dropNode(id), dropNode(id))
	}
	drainOps = append(drainOps, closestNode(zero))
	for index, id := range drainIDs {
		drainOps = append(drainOps, putNode(id, addr("2001:db8::30", uint16(8000+index), uint32(index+1))))
	}
	drainOps = append(drainOps, closestNode(zero))

	scenarios := []struct {
		id    string
		input nodeTableInput
	}{
		{"empty_origin_and_invalid_address", nodeTableInput{Origin: origin, Operations: []nodeTableOperation{
			{Kind: "origin"}, closestNode(origin), closestNode(nodeID(99)),
			{Kind: "putInvalid", ID: nodeID(1)}, putNode(origin, addr("127.0.0.1", 0, 0)),
			dropNode(origin),
		}}},
		{"address_representations_updates_and_shared_endpoint", nodeTableInput{Origin: zero, Operations: addressOps}},
		{"bucket_capacity_eighty_duplicate_and_reopen", nodeTableInput{Origin: zero, Operations: capacityOps}},
		{"nonzero_origin_forward_traversal", nodeTableInput{Origin: origin, Operations: forwardOps}},
		{"nonzero_origin_reverse_traversal", nodeTableInput{Origin: origin, Operations: reverseOps}},
		{"drain_and_second_cycle", nodeTableInput{Origin: zero, Operations: drainOps}},
	}

	fixtures := make([]nodeTableFixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixtures = append(fixtures, runNodeTableScenario(t, scenario.id, scenario.input))
	}
	reconcileNodeTableFixtures(t, fixtures)
}

func runNodeTableScenario(t *testing.T, id string, input nodeTableInput) nodeTableFixture {
	t.Helper()
	origin := protocol.MustParseID(input.Origin)
	table := New(Params{NodeID: origin}).Table.(*table)
	expected := nodeTableExpected{Results: make([]nodeTableResult, 0, len(input.Operations))}
	for _, operation := range input.Operations {
		result := nodeTableResult{}
		var nodeID protocol.ID
		if operation.ID != "" {
			nodeID = protocol.MustParseID(operation.ID)
		}
		switch operation.Kind {
		case "origin":
			result.Origin = table.Origin().String()
		case "put":
			result.PutResult = table.PutNode(nodeID, operation.Addr.addrPort()).String()
		case "putInvalid":
			result.PutResult = table.PutNode(nodeID, netip.AddrPort{}).String()
			result.RustUnsupported = true
		case "drop":
			value := table.DropNode(nodeID, nil)
			result.DropResult = &value
		case "closest":
			value := projectNodeTableNodes(table.GetClosestNodes(nodeID))
			result.Closest = &value
		default:
			t.Fatalf("%s: unknown operation %q", id, operation.Kind)
		}
		result.Count = table.nodes.count()
		result.State = nodeTableState(table)
		expected.Results = append(expected.Results, result)
	}
	return nodeTableFixture{ID: id, Subsystem: "dht_node_table", Input: input, Expected: expected}
}

func nodeTableState(table *table) []nodeTableNode {
	state := make([]nodeTableNode, 0, len(table.nodes.items))
	for _, node := range table.nodes.items {
		state = append(state, projectNodeTableNode(node))
	}
	sort.Slice(state, func(i, j int) bool { return state[i].ID < state[j].ID })
	return state
}

func projectNodeTableNodes(nodes []Node) []nodeTableNode {
	result := make([]nodeTableNode, len(nodes))
	for index, node := range nodes {
		result[index] = projectNodeTableNode(node)
	}
	return result
}

func projectNodeTableNode(node Node) nodeTableNode {
	return nodeTableNode{ID: node.ID().String(), Addr: projectNodeTableAddress(node.Addr())}
}

func projectNodeTableAddress(value netip.AddrPort) nodeTableAddress {
	scope := uint32(0)
	if value.Addr().Zone() != "" {
		var parsed uint64
		for _, char := range value.Addr().Zone() {
			parsed = parsed*10 + uint64(char-'0')
		}
		scope = uint32(parsed)
	}
	return nodeTableAddress{IP: value.Addr().WithZone("").String(), Port: value.Port(), Scope: scope}
}

func (value nodeTableAddress) addrPort() netip.AddrPort {
	ip := netip.MustParseAddr(value.IP)
	if value.Scope != 0 {
		ip = ip.WithZone(uintString(value.Scope))
	}
	return netip.AddrPortFrom(ip, value.Port)
}

func addr(ip string, port uint16, scope uint32) *nodeTableAddress {
	return &nodeTableAddress{IP: ip, Port: port, Scope: scope}
}

func putNode(id string, address *nodeTableAddress) nodeTableOperation {
	return nodeTableOperation{Kind: "put", ID: id, Addr: address}
}

func dropNode(id string) nodeTableOperation { return nodeTableOperation{Kind: "drop", ID: id} }
func closestNode(id string) nodeTableOperation {
	return nodeTableOperation{Kind: "closest", ID: id}
}

func nodeID(value int) string { return nodeIDWithFirstByte(0, value) }

func nodeIDWithFirstByte(first byte, value int) string {
	bytes := [20]byte{first}
	bytes[18] = byte(value >> 8)
	bytes[19] = byte(value)
	return protocol.ID(bytes).String()
}

func idAtDistance(origin, distance string) string {
	left := protocol.MustParseID(origin)
	right := protocol.MustParseID(distance)
	for index := range left {
		left[index] ^= right[index]
	}
	return left.String()
}

func uintString(value uint32) string {
	if value == 0 {
		return "0"
	}
	var buffer [10]byte
	index := len(buffer)
	for value != 0 {
		index--
		buffer[index] = byte(value%10) + '0'
		value /= 10
	}
	return string(buffer[index:])
}

func reconcileNodeTableFixtures(t *testing.T, fixtures []nodeTableFixture) {
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
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve generator source")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../testdata/parity/dht/node_table.jsonl"))
	if *updateDHTNodeTableParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-node-table-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("node-table fixture is stale; rerun with -update-dht-node-table-parity")
	}
}
