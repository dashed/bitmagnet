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

var updateDHTKTableCoreParity = flag.Bool(
	"update-dht-ktable-core-parity",
	false,
	"rewrite the Rust DHT KTable core parity fixture",
)

type ktableCoreFixture struct {
	ID        string             `json:"id"`
	Subsystem string             `json:"subsystem"`
	Input     ktableCoreInput    `json:"input"`
	Expected  ktableCoreExpected `json:"expected"`
}

type ktableCoreInput struct {
	Origin          string                `json:"origin"`
	AddressUniverse []ktableCoreAddress   `json:"addressUniverse"`
	Operations      []ktableCoreOperation `json:"operations"`
}

type ktableCoreOperation struct {
	Kind  string              `json:"kind"`
	ID    string              `json:"id,omitempty"`
	Addr  *ktableCoreAddress  `json:"addr,omitempty"`
	Peers []ktableCoreAddress `json:"peers,omitempty"`
	Addrs []ktableCoreAddress `json:"addrs,omitempty"`
}

type ktableCoreAddress struct {
	IP    string `json:"ip"`
	Port  uint16 `json:"port"`
	Scope uint32 `json:"scope,omitempty"`
}

type ktableCoreExpected struct {
	Results []ktableCoreResult `json:"results"`
}

type ktableCoreResult struct {
	PutResult    string               `json:"putResult,omitempty"`
	BoolResult   *bool                `json:"boolResult,omitempty"`
	Filtered     *[]ktableCoreAddress `json:"filtered,omitempty"`
	Lookup       *ktableCoreLookup    `json:"lookup,omitempty"`
	NodeCount    int                  `json:"nodeCount"`
	HashCount    int                  `json:"hashCount"`
	ReverseCount int                  `json:"reverseCount"`
	Nodes        []ktableCoreNode     `json:"nodes"`
	Hashes       []ktableCoreHash     `json:"hashes"`
	KnownAddrs   []ktableCoreAddress  `json:"knownAddrs"`
	Reverse      []ktableCoreReverse  `json:"reverse"`
}

type ktableCoreLookup struct {
	Found        bool             `json:"found"`
	Hash         *ktableCoreHash  `json:"hash,omitempty"`
	ClosestNodes []ktableCoreNode `json:"closestNodes"`
}

type ktableCoreNode struct {
	ID   string            `json:"id"`
	Addr ktableCoreAddress `json:"addr"`
}

type ktableCoreHash struct {
	ID    string              `json:"id"`
	Peers []ktableCoreAddress `json:"peers"`
}

type ktableCoreReverse struct {
	Addr   ktableCoreAddress `json:"addr"`
	PeerID *string           `json:"peerId,omitempty"`
	Hashes []string          `json:"hashes"`
}

func TestGenerateDHTKTableCoreParity(t *testing.T) {
	nonzeroOrigin := "00112233445566778899aabbccddeeff10203040"
	zero := protocol.ID{}.String()
	v4a := ktableCoreAddr("192.0.2.1", 100, 0)
	v4b := ktableCoreAddr("198.51.100.2", 200, 0)
	v4unknown := ktableCoreAddr("203.0.113.9", 0, 0)
	mapped := ktableCoreAddr("::ffff:192.0.2.1", 300, 0)
	scope7 := ktableCoreAddr("fe80::1", 400, 7)
	scope8 := ktableCoreAddr("fe80::1", 500, 8)
	native := ktableCoreAddr("2001:db8::1", 600, 0)

	sharedOps := []ktableCoreOperation{
		ktableCorePutNode(ktableCoreID(1), v4a),
		ktableCoreFilter(ktableCoreIP(v4a), ktableCoreIP(v4a), v4unknown),
		ktableCoreDropAddr(ktableCoreWithPort(v4a, 65535)),
		ktableCorePutNode(ktableCoreID(1), v4a),
		ktableCoreFilter(ktableCoreWithPort(v4a, 0), v4unknown),
		ktableCorePutHash(ktableCoreID(20), v4a),
		ktableCorePutNode(ktableCoreID(2), ktableCoreWithPort(v4a, 300)),
		ktableCorePutNode(ktableCoreID(2), ktableCoreWithPort(v4a, 300)),
		ktableCoreDropAddr(ktableCoreWithPort(v4a, 999)),
		ktableCoreLookupOp(ktableCoreID(20)),
		ktableCoreFilter(ktableCoreIP(v4a)),
		ktableCoreDropNode(ktableCoreID(1)),
		ktableCorePutNode(zero, v4b),
		ktableCorePutNode(zero, v4b),
		ktableCoreFilter(ktableCoreWithPort(v4b, 1)),
		ktableCoreDropAddr(ktableCoreWithPort(v4b, 2)),
		ktableCoreDropNode(zero),
	}

	identityOps := []ktableCoreOperation{
		ktableCorePutHash(ktableCoreID(30), v4a, mapped, scope7, scope8, native),
		ktableCoreFilter(
			ktableCoreWithPort(v4a, 1),
			ktableCoreWithPort(mapped, 2),
			ktableCoreWithPort(scope7, 3),
			ktableCoreWithPort(scope8, 4),
			v4unknown,
		),
		ktableCorePutNode(ktableCoreID(3), ktableCoreWithPort(scope7, 10)),
		ktableCorePutNode(ktableCoreID(3), ktableCoreWithPort(scope7, 10)),
		ktableCorePutNode(ktableCoreID(3), ktableCoreWithPort(scope7, 11)),
		ktableCoreFilter(ktableCoreIP(scope7), ktableCoreIP(scope8)),
		ktableCorePutNode(ktableCoreID(3), ktableCoreWithPort(scope8, 11)),
		ktableCoreFilter(ktableCoreIP(scope7), ktableCoreIP(scope8)),
	}

	emptyHashID := ktableCoreID(40)
	lookupOps := []ktableCoreOperation{
		ktableCorePutNode(ktableCoreID(4), ktableCoreAddr("192.0.2.4", 4, 0)),
		ktableCorePutNode(ktableCoreID(5), ktableCoreAddr("192.0.2.5", 5, 0)),
		ktableCorePutHash(emptyHashID),
		ktableCoreLookupOp(emptyHashID),
		ktableCorePutHash(
			emptyHashID,
			ktableCoreAddr("192.0.2.44", 1, 0),
			ktableCoreAddr("192.0.2.44", 2, 0),
			mapped,
		),
		ktableCorePutHash(
			emptyHashID,
			ktableCoreAddr("192.0.2.44", 3, 0),
			native,
		),
		ktableCoreLookupOp(emptyHashID),
		ktableCoreLookupOp(ktableCoreID(41)),
	}

	capacityOps := make([]ktableCoreOperation, 0, 86)
	capacityIDs := make([]string, 81)
	for index := range capacityIDs {
		if index == 80 {
			capacityIDs[index] = "ffffffffffffffffffffffffffffffffffffffff"
		} else {
			capacityIDs[index] = ktableCoreIDWithFirstByte(0xc0, index+1)
		}
		capacityOps = append(capacityOps, ktableCorePutHash(capacityIDs[index]))
	}
	rejectedPeer := ktableCoreAddr("203.0.113.81", 81, 0)
	capacityOps[len(capacityOps)-1].Peers = []ktableCoreAddress{rejectedPeer}
	capacityOps = append(capacityOps,
		ktableCoreFilter(ktableCoreIP(rejectedPeer)),
		ktableCorePutHash(capacityIDs[0], ktableCoreAddr("203.0.113.1", 1, 0)),
		ktableCorePutHash(capacityIDs[0], ktableCoreAddr("203.0.113.1", 65535, 0)),
		ktableCoreLookupOp(capacityIDs[0]),
		ktableCoreLookupOp(capacityIDs[80]),
	)

	scenarios := []struct {
		id    string
		input ktableCoreInput
	}{
		{
			"node_reverse_omission_zero_sentinel_and_destructive_drop",
			ktableCoreInput{
				Origin: nonzeroOrigin,
				AddressUniverse: []ktableCoreAddress{
					ktableCoreIP(v4a), ktableCoreIP(v4b), v4unknown,
				},
				Operations: sharedOps,
			},
		},
		{
			"ip_only_mapped_native_and_scope_identity",
			ktableCoreInput{
				Origin: zero,
				AddressUniverse: []ktableCoreAddress{
					ktableCoreIP(v4a), ktableCoreIP(mapped), ktableCoreIP(scope7),
					ktableCoreIP(scope8), ktableCoreIP(native), v4unknown,
				},
				Operations: identityOps,
			},
		},
		{
			"empty_hash_accumulation_last_port_and_closest_fallback",
			ktableCoreInput{
				Origin: zero,
				AddressUniverse: []ktableCoreAddress{
					ktableCoreAddr("192.0.2.44", 0, 0), ktableCoreIP(mapped),
					ktableCoreIP(native),
				},
				Operations: lookupOps,
			},
		},
		{
			"hash_capacity_rejection_and_duplicate_immutability",
			ktableCoreInput{
				Origin: zero,
				AddressUniverse: []ktableCoreAddress{
					ktableCoreIP(rejectedPeer), ktableCoreAddr("203.0.113.1", 0, 0),
				},
				Operations: capacityOps,
			},
		},
	}

	fixtures := make([]ktableCoreFixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixtures = append(fixtures, runKTableCoreScenario(t, scenario.id, scenario.input))
	}
	reconcileKTableCoreFixtures(t, fixtures)
}

func runKTableCoreScenario(t *testing.T, id string, input ktableCoreInput) ktableCoreFixture {
	t.Helper()
	table := New(Params{NodeID: protocol.MustParseID(input.Origin)}).Table.(*table)
	expected := ktableCoreExpected{Results: make([]ktableCoreResult, 0, len(input.Operations))}
	for _, operation := range input.Operations {
		result := ktableCoreResult{}
		var operationID protocol.ID
		if operation.ID != "" {
			operationID = protocol.MustParseID(operation.ID)
		}
		switch operation.Kind {
		case "putNode":
			result.PutResult = table.PutNode(operationID, operation.Addr.addrPort()).String()
		case "dropNode":
			value := table.DropNode(operationID, nil)
			result.BoolResult = &value
		case "dropAddr":
			value := DropAddr{Addr: operation.Addr.addrPort().Addr()}.execReturn(table)
			result.BoolResult = &value
		case "putHash":
			peers := make([]HashPeer, len(operation.Peers))
			for index, peer := range operation.Peers {
				peers[index] = HashPeer{Addr: peer.addrPort()}
			}
			result.PutResult = table.PutHash(operationID, peers).String()
		case "filter":
			addrs := make([]netip.Addr, len(operation.Addrs))
			for index, addr := range operation.Addrs {
				addrs[index] = addr.addrPort().Addr()
			}
			filtered := table.FilterKnownAddrs(addrs)
			projected := make([]ktableCoreAddress, len(filtered))
			for index, addr := range filtered {
				projected[index] = ktableCoreProjectIP(addr)
			}
			result.Filtered = &projected
		case "lookup":
			lookup := table.GetHashOrClosestNodes(operationID)
			projected := ktableCoreLookup{Found: lookup.Found}
			if lookup.Found {
				hash := ktableCoreProjectHash(lookup.Hash)
				projected.Hash = &hash
			} else {
				projected.ClosestNodes = ktableCoreProjectNodes(lookup.ClosestNodes)
			}
			result.Lookup = &projected
		default:
			t.Fatalf("%s: unknown operation %q", id, operation.Kind)
		}
		result.NodeCount = table.nodes.count()
		result.HashCount = table.hashes.count()
		result.ReverseCount = table.addrs.len()
		result.Nodes = ktableCoreNodeState(table)
		result.Hashes = ktableCoreHashState(table)
		result.KnownAddrs = ktableCoreKnownAddrs(table)
		result.Reverse = ktableCoreReverseState(table)
		expected.Results = append(expected.Results, result)
	}
	return ktableCoreFixture{ID: id, Subsystem: "dht_ktable_core", Input: input, Expected: expected}
}

func ktableCoreNodeState(table *table) []ktableCoreNode {
	state := make([]ktableCoreNode, 0, len(table.nodes.items))
	for _, node := range table.nodes.items {
		state = append(state, ktableCoreNode{ID: node.ID().String(), Addr: ktableCoreProjectAddr(node.Addr())})
	}
	sort.Slice(state, func(i, j int) bool { return state[i].ID < state[j].ID })
	return state
}

func ktableCoreHashState(table *table) []ktableCoreHash {
	state := make([]ktableCoreHash, 0, len(table.hashes.items))
	for _, hash := range table.hashes.items {
		state = append(state, ktableCoreProjectHash(hash))
	}
	sort.Slice(state, func(i, j int) bool { return state[i].ID < state[j].ID })
	return state
}

func ktableCoreProjectHash(hash Hash) ktableCoreHash {
	peers := hash.Peers()
	projected := make([]ktableCoreAddress, len(peers))
	for index, peer := range peers {
		projected[index] = ktableCoreProjectAddr(peer.Addr)
	}
	sort.Slice(projected, func(i, j int) bool {
		return projected[i].addrPort().String() < projected[j].addrPort().String()
	})
	return ktableCoreHash{ID: hash.ID().String(), Peers: projected}
}

func ktableCoreProjectNodes(nodes []Node) []ktableCoreNode {
	projected := make([]ktableCoreNode, len(nodes))
	for index, node := range nodes {
		projected[index] = ktableCoreNode{ID: node.ID().String(), Addr: ktableCoreProjectAddr(node.Addr())}
	}
	return projected
}

func ktableCoreKnownAddrs(table *table) []ktableCoreAddress {
	known := make([]ktableCoreAddress, 0, len(table.addrs.addrs))
	for key := range table.addrs.addrs {
		known = append(known, ktableCoreProjectIP(netip.MustParseAddr(key)))
	}
	sort.Slice(known, func(i, j int) bool {
		return known[i].addrPort().Addr().String() < known[j].addrPort().Addr().String()
	})
	return known
}

func ktableCoreReverseState(table *table) []ktableCoreReverse {
	state := make([]ktableCoreReverse, 0, len(table.addrs.addrs))
	for key, info := range table.addrs.addrs {
		entry := ktableCoreReverse{
			Addr:   ktableCoreProjectIP(netip.MustParseAddr(key)),
			Hashes: make([]string, 0, len(info.hashes)),
		}
		if !info.peerID.IsZero() {
			peerID := info.peerID.String()
			entry.PeerID = &peerID
		}
		for hash := range info.hashes {
			entry.Hashes = append(entry.Hashes, hash.String())
		}
		sort.Strings(entry.Hashes)
		state = append(state, entry)
	}
	sort.Slice(state, func(i, j int) bool {
		return state[i].Addr.addrPort().Addr().String() < state[j].Addr.addrPort().Addr().String()
	})
	return state
}

func ktableCoreProjectAddr(value netip.AddrPort) ktableCoreAddress {
	projected := ktableCoreProjectIP(value.Addr())
	projected.Port = value.Port()
	return projected
}

func ktableCoreProjectIP(value netip.Addr) ktableCoreAddress {
	scope := uint32(0)
	if value.Zone() != "" {
		for _, char := range value.Zone() {
			scope = scope*10 + uint32(char-'0')
		}
	}
	return ktableCoreAddress{IP: value.WithZone("").String(), Scope: scope}
}

func (value ktableCoreAddress) addrPort() netip.AddrPort {
	ip := netip.MustParseAddr(value.IP)
	if value.Scope != 0 {
		ip = ip.WithZone(uintString(value.Scope))
	}
	return netip.AddrPortFrom(ip, value.Port)
}

func ktableCoreAddr(ip string, port uint16, scope uint32) ktableCoreAddress {
	return ktableCoreAddress{IP: ip, Port: port, Scope: scope}
}

func ktableCoreIP(addr ktableCoreAddress) ktableCoreAddress {
	addr.Port = 0
	return addr
}

func ktableCoreWithPort(addr ktableCoreAddress, port uint16) ktableCoreAddress {
	addr.Port = port
	return addr
}

func ktableCorePutNode(id string, addr ktableCoreAddress) ktableCoreOperation {
	return ktableCoreOperation{Kind: "putNode", ID: id, Addr: &addr}
}

func ktableCoreDropNode(id string) ktableCoreOperation {
	return ktableCoreOperation{Kind: "dropNode", ID: id}
}

func ktableCoreDropAddr(addr ktableCoreAddress) ktableCoreOperation {
	return ktableCoreOperation{Kind: "dropAddr", Addr: &addr}
}

func ktableCorePutHash(id string, peers ...ktableCoreAddress) ktableCoreOperation {
	return ktableCoreOperation{Kind: "putHash", ID: id, Peers: peers}
}

func ktableCoreFilter(addrs ...ktableCoreAddress) ktableCoreOperation {
	return ktableCoreOperation{Kind: "filter", Addrs: addrs}
}

func ktableCoreLookupOp(id string) ktableCoreOperation {
	return ktableCoreOperation{Kind: "lookup", ID: id}
}

func ktableCoreID(value int) string { return ktableCoreIDWithFirstByte(0, value) }

func ktableCoreIDWithFirstByte(first byte, value int) string {
	bytes := [20]byte{first}
	bytes[18] = byte(value >> 8)
	bytes[19] = byte(value)
	return protocol.ID(bytes).String()
}

func reconcileKTableCoreFixtures(t *testing.T, fixtures []ktableCoreFixture) {
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
	path := filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../testdata/parity/dht/ktable_core.jsonl"))
	if *updateDHTKTableCoreParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-ktable-core-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("KTable core fixture is stale; rerun with -update-dht-ktable-core-parity")
	}
}
