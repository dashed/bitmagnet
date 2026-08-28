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
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

var updateDHTKTableTemporalParity = flag.Bool(
	"update-dht-ktable-temporal-parity",
	false,
	"rewrite the Rust DHT temporal KTable parity fixture",
)

type ktableTemporalFixture struct {
	ID        string                 `json:"id"`
	Subsystem string                 `json:"subsystem"`
	Input     ktableTemporalInput    `json:"input"`
	Expected  ktableTemporalExpected `json:"expected"`
}

type ktableTemporalInput struct {
	Origin     string                    `json:"origin"`
	Operations []ktableTemporalOperation `json:"operations"`
}

type ktableTemporalOperation struct {
	Kind               string                    `json:"kind"`
	ID                 string                    `json:"id,omitempty"`
	Addr               string                    `json:"addr,omitempty"`
	Options            []ktableTemporalOption    `json:"options,omitempty"`
	Capture            string                    `json:"capture,omitempty"`
	Handle             string                    `json:"handle,omitempty"`
	CutoffHandle       string                    `json:"cutoffHandle,omitempty"`
	CutoffDelaySeconds int64                     `json:"cutoffDelaySeconds,omitempty"`
	Limit              int                       `json:"limit,omitempty"`
	Commands           []ktableTemporalOperation `json:"commands,omitempty"`
}

type ktableTemporalOption struct {
	Kind             string `json:"kind"`
	Supported        *bool  `json:"supported,omitempty"`
	DiscoveredNum    int64  `json:"discoveredNum,omitempty"`
	TotalNum         int64  `json:"totalNum,omitempty"`
	NextDelaySeconds int64  `json:"nextDelaySeconds,omitempty"`
}

type ktableTemporalExpected struct {
	Results []ktableTemporalResult `json:"results"`
}

type ktableTemporalResult struct {
	PutResult   string                     `json:"putResult,omitempty"`
	BoolResult  *bool                      `json:"boolResult,omitempty"`
	NodePresent *bool                      `json:"nodePresent,omitempty"`
	QueryIDs    *[]string                  `json:"queryIds,omitempty"`
	Handle      *ktableTemporalHandleState `json:"handle,omitempty"`
	NodeCount   int                        `json:"nodeCount"`
	HashCount   int                        `json:"hashCount"`
	Sample      *ktableTemporalSampleState `json:"sample,omitempty"`
}

type ktableTemporalHandleState struct {
	ID                string `json:"id"`
	Addr              string `json:"addr"`
	LastResponded     bool   `json:"lastResponded"`
	Dropped           bool   `json:"dropped"`
	Bep51Support      string `json:"bep51Support"`
	SampledNum        int64  `json:"sampledNum"`
	LastDiscoveredNum int64  `json:"lastDiscoveredNum"`
	TotalNum          int64  `json:"totalNum"`
	NextSampleState   string `json:"nextSampleState"`
	Candidate         bool   `json:"candidate"`
}

type ktableTemporalSampleState struct {
	HashCount       int  `json:"hashCount"`
	NodeCount       int  `json:"nodeCount"`
	TotalHashes     int  `json:"totalHashes"`
	HashIDsUnique   bool `json:"hashIdsUnique"`
	NodeIDsUnique   bool `json:"nodeIdsUnique"`
	HashesAreSubset bool `json:"hashesAreSubset"`
	NodesAreSubset  bool `json:"nodesAreSubset"`
}

func TestGenerateDHTKTableTemporalParity(t *testing.T) {
	fixtures := []ktableTemporalFixture{
		runKTableTemporalScenario(t, "live_generation_options_and_temporal_queries", ktableTemporalLiveInput()),
		runKTableTemporalScenario(t, "capacity_rejection_does_not_apply_options", ktableTemporalCapacityInput()),
	}
	reconcileKTableTemporalFixtures(t, fixtures)
}

func ktableTemporalLiveInput() ktableTemporalInput {
	firstID := ktableTemporalID(1)
	secondID := ktableTemporalID(2)
	thirdID := ktableTemporalID(3)
	fourthID := ktableTemporalID(4)
	fifthID := ktableTemporalID(5)
	wrappingID := ktableTemporalID(6)
	hashID := ktableTemporalIDWithFirstByte(0xa0, 1)
	return ktableTemporalInput{
		Origin: protocol.ID{}.String(),
		Operations: []ktableTemporalOperation{
			ktableTemporalPut(firstID, "192.0.2.1:100", "old",
				ktableTemporalSupport(true),
				ktableTemporalSample(3, 100, -60),
			),
			{Kind: "observe", Handle: "old"},
			ktableTemporalPut(firstID, "192.0.2.1:101", "alias",
				ktableTemporalSample(2, 90, -30),
				ktableTemporalResponded(),
				ktableTemporalSupport(false),
				ktableTemporalSupport(true),
			),
			{Kind: "observe", Handle: "old"},
			{Kind: "observe", Handle: "alias"},
			{Kind: "oldest", CutoffHandle: "old", Limit: 80},
			{Kind: "oldest", CutoffDelaySeconds: 60, Limit: 80},
			{Kind: "dropNode", ID: firstID},
			{Kind: "observe", Handle: "old"},
			{Kind: "nodePresent", ID: firstID},
			ktableTemporalPut(firstID, "192.0.2.1:102", "new", ktableTemporalSupport(false)),
			{Kind: "observe", Handle: "old"},
			{Kind: "observe", Handle: "new"},
			ktableTemporalPut(secondID, "198.51.100.2:200", "second"),
			ktableTemporalPut(secondID, "198.51.100.2:200", ""),
			ktableTemporalPut(thirdID, "198.51.100.3:300", "third", ktableTemporalSample(1, 10, -1)),
			ktableTemporalPut(fourthID, "198.51.100.4:400", "fourth", ktableTemporalSample(0, 50, -60)),
			{Kind: "candidates", Limit: 80},
			{Kind: "observe", Handle: "fourth"},
			ktableTemporalPut(thirdID, "198.51.100.3:301", "", ktableTemporalSample(2, 90, -30)),
			{Kind: "observe", Handle: "third"},
			{Kind: "candidates", Limit: 80},
			ktableTemporalPut(thirdID, "198.51.100.3:301", "", ktableTemporalResponded()),
			{Kind: "candidates", Limit: 80},
			{Kind: "oldest", CutoffHandle: "third", Limit: 80},
			{Kind: "dropAddr", Addr: "198.51.100.2:65535"},
			{Kind: "observe", Handle: "second"},
			ktableTemporalPut(wrappingID, "198.51.100.6:600", "wrapping",
				ktableTemporalSample(int64(^uint64(0)>>1), -int64(^uint64(0)>>1)-1, -1),
			),
			ktableTemporalPut(wrappingID, "198.51.100.6:601", "",
				ktableTemporalSample(1, -1, -1),
			),
			{Kind: "observe", Handle: "wrapping"},
			{
				Kind: "batch",
				Commands: []ktableTemporalOperation{
					ktableTemporalPut(fifthID, "198.51.100.5:500", "", ktableTemporalSupport(true)),
					ktableTemporalPut(fifthID, "198.51.100.5:501", ""),
					{Kind: "putHash", ID: hashID},
					{Kind: "dropNode", ID: fourthID},
					{Kind: "dropAddr", Addr: "198.51.100.5:65535"},
				},
			},
			{Kind: "observe", Handle: "fourth"},
			{Kind: "nodePresent", ID: fifthID},
			{Kind: "sample"},
		},
	}
}

func ktableTemporalCapacityInput() ktableTemporalInput {
	operations := make([]ktableTemporalOperation, 0, 84)
	for index := 1; index <= nodesK; index++ {
		capture := ""
		if index == 1 {
			capture = "first"
		}
		operations = append(operations, ktableTemporalPut(
			ktableTemporalIDWithFirstByte(0xc0, index),
			netip.AddrPortFrom(netip.AddrFrom4([4]byte{203, 0, 113, byte(index)}), uint16(index)).String(),
			capture,
		))
	}
	rejectedID := "ffffffffffffffffffffffffffffffffffffffff"
	operations = append(operations,
		ktableTemporalPut(rejectedID, "203.0.113.250:65535", "rejected",
			ktableTemporalResponded(),
			ktableTemporalSupport(false),
			ktableTemporalSample(7, 700, -60),
		),
		ktableTemporalOperation{Kind: "nodePresent", ID: rejectedID},
		ktableTemporalPut(ktableTemporalIDWithFirstByte(0xc0, 1), "203.0.113.1:60001", "first",
			ktableTemporalSupport(false),
		),
		ktableTemporalOperation{Kind: "observe", Handle: "first"},
	)
	hashCommands := make([]ktableTemporalOperation, 25)
	for index := range hashCommands {
		hashCommands[index] = ktableTemporalOperation{
			Kind: "putHash",
			ID:   ktableTemporalIDWithFirstByte(0xa0, index+1),
		}
	}
	operations = append(operations,
		ktableTemporalOperation{Kind: "batch", Commands: hashCommands},
		ktableTemporalOperation{Kind: "sample"},
	)
	return ktableTemporalInput{Origin: protocol.ID{}.String(), Operations: operations}
}

func runKTableTemporalScenario(t *testing.T, id string, input ktableTemporalInput) ktableTemporalFixture {
	t.Helper()
	table := New(Params{NodeID: protocol.MustParseID(input.Origin)}).Table.(*table)
	handles := make(map[string]*node)
	expected := ktableTemporalExpected{Results: make([]ktableTemporalResult, 0, len(input.Operations))}
	for index, operation := range input.Operations {
		result := ktableTemporalResult{}
		var operationID protocol.ID
		if operation.ID != "" {
			operationID = protocol.MustParseID(operation.ID)
		}
		switch operation.Kind {
		case "putNode":
			options := make([]NodeOption, len(operation.Options))
			for optionIndex, option := range operation.Options {
				options[optionIndex] = ktableTemporalNodeOption(t, id, index, option)
			}
			result.PutResult = table.PutNode(operationID, netip.MustParseAddrPort(operation.Addr), options...).String()
			if operation.Capture != "" {
				handle, ok := table.nodes.items[operationID]
				if ok {
					handles[operation.Capture] = handle
				}
			}
		case "dropNode":
			value := table.DropNode(operationID, nil)
			result.BoolResult = &value
		case "dropAddr":
			value := DropAddr{Addr: netip.MustParseAddrPort(operation.Addr).Addr()}.execReturn(table)
			result.BoolResult = &value
		case "putHash":
			result.PutResult = table.PutHash(operationID, nil).String()
		case "batch":
			commands := make([]Command, len(operation.Commands))
			for commandIndex, command := range operation.Commands {
				commands[commandIndex] = ktableTemporalCommand(t, id, index, commandIndex, command)
			}
			table.BatchCommand(commands...)
		case "observe":
			handle, ok := handles[operation.Handle]
			if !ok {
				t.Fatalf("%s operation %d: unknown handle %q", id, index, operation.Handle)
			}
			state := ktableTemporalProjectHandle(handle)
			result.Handle = &state
		case "nodePresent":
			_, value := table.nodes.items[operationID]
			result.NodePresent = &value
		case "oldest":
			var cutoff time.Time
			if operation.CutoffHandle != "" {
				handle, ok := handles[operation.CutoffHandle]
				if !ok {
					t.Fatalf("%s operation %d: unknown cutoff handle %q", id, index, operation.CutoffHandle)
				}
				cutoff = handle.Time()
			} else {
				cutoff = time.Now().Add(time.Duration(operation.CutoffDelaySeconds) * time.Second)
			}
			ids := ktableTemporalProjectIDs(table.GetOldestNodes(cutoff, operation.Limit))
			result.QueryIDs = &ids
		case "candidates":
			ids := ktableTemporalProjectIDs(table.GetNodesForSampleInfoHashes(operation.Limit))
			result.QueryIDs = &ids
		case "sample":
			state := ktableTemporalProjectSample(table, table.SampleHashesAndNodes())
			result.Sample = &state
		default:
			t.Fatalf("%s operation %d: unknown kind %q", id, index, operation.Kind)
		}
		result.NodeCount = table.nodes.count()
		result.HashCount = table.hashes.count()
		expected.Results = append(expected.Results, result)
	}
	return ktableTemporalFixture{ID: id, Subsystem: "dht_ktable_temporal", Input: input, Expected: expected}
}

func ktableTemporalCommand(
	t *testing.T,
	scenarioID string,
	operationIndex int,
	commandIndex int,
	operation ktableTemporalOperation,
) Command {
	t.Helper()
	var operationID protocol.ID
	if operation.ID != "" {
		operationID = protocol.MustParseID(operation.ID)
	}
	switch operation.Kind {
	case "putNode":
		options := make([]NodeOption, len(operation.Options))
		for optionIndex, option := range operation.Options {
			options[optionIndex] = ktableTemporalNodeOption(t, scenarioID, operationIndex, option)
		}
		return PutNode{ID: operationID, Addr: netip.MustParseAddrPort(operation.Addr), Options: options}
	case "dropNode":
		return DropNode{ID: operationID}
	case "dropAddr":
		return DropAddr{Addr: netip.MustParseAddrPort(operation.Addr).Addr()}
	case "putHash":
		return PutHash{ID: operationID}
	default:
		t.Fatalf(
			"%s operation %d command %d: unknown kind %q",
			scenarioID,
			operationIndex,
			commandIndex,
			operation.Kind,
		)
		return nil
	}
}

func ktableTemporalNodeOption(
	t *testing.T,
	scenarioID string,
	operationIndex int,
	option ktableTemporalOption,
) NodeOption {
	t.Helper()
	switch option.Kind {
	case "responded":
		return NodeResponded()
	case "support":
		if option.Supported == nil {
			t.Fatalf("%s operation %d: support option missing value", scenarioID, operationIndex)
		}
		return NodeBep51Support(*option.Supported)
	case "sample":
		return NodeSampleInfoHashesRes(
			int(option.DiscoveredNum),
			int(option.TotalNum),
			time.Now().Add(time.Duration(option.NextDelaySeconds)*time.Second),
		)
	default:
		t.Fatalf("%s operation %d: unknown option %q", scenarioID, operationIndex, option.Kind)
		return nil
	}
}

func ktableTemporalProjectHandle(value *node) ktableTemporalHandleState {
	now := time.Now()
	nextState := "zero"
	if !value.nextSampleInfoHashesTime.IsZero() {
		nextState = "future"
		if value.nextSampleInfoHashesTime.Before(now) {
			nextState = "past"
		}
	}
	support := "unknown"
	switch value.bep51Support {
	case protocolSupportYes:
		support = "yes"
	case protocolSupportNo:
		support = "no"
	}
	return ktableTemporalHandleState{
		ID:                value.ID().String(),
		Addr:              value.Addr().String(),
		LastResponded:     !value.Time().IsZero(),
		Dropped:           value.Dropped(),
		Bep51Support:      support,
		SampledNum:        int64(value.sampledNum),
		LastDiscoveredNum: int64(value.lastDiscoveredNum),
		TotalNum:          int64(value.totalNum),
		NextSampleState:   nextState,
		Candidate:         value.IsSampleInfoHashesCandidate(),
	}
}

func ktableTemporalProjectSample(table *table, sample SampleHashesAndNodesResult) ktableTemporalSampleState {
	hashIDs := make(map[ID]struct{}, len(sample.Hashes))
	hashIDsUnique := true
	hashesAreSubset := true
	for _, value := range sample.Hashes {
		id := value.ID()
		if _, exists := hashIDs[id]; exists {
			hashIDsUnique = false
		}
		hashIDs[id] = struct{}{}
		stored, exists := table.hashes.items[id]
		if !exists || stored.public() != value {
			hashesAreSubset = false
		}
	}
	nodeIDs := make(map[ID]struct{}, len(sample.Nodes))
	nodeIDsUnique := true
	nodesAreSubset := true
	for _, value := range sample.Nodes {
		id := value.ID()
		if _, exists := nodeIDs[id]; exists {
			nodeIDsUnique = false
		}
		nodeIDs[id] = struct{}{}
		stored, exists := table.nodes.items[id]
		if !exists || stored.public() != value {
			nodesAreSubset = false
		}
	}
	return ktableTemporalSampleState{
		HashCount:       len(sample.Hashes),
		NodeCount:       len(sample.Nodes),
		TotalHashes:     sample.TotalHashes,
		HashIDsUnique:   hashIDsUnique,
		NodeIDsUnique:   nodeIDsUnique,
		HashesAreSubset: hashesAreSubset,
		NodesAreSubset:  nodesAreSubset,
	}
}

func ktableTemporalProjectIDs(nodes []Node) []string {
	ids := make([]string, len(nodes))
	for index, value := range nodes {
		ids[index] = value.ID().String()
	}
	// Oldest ties and candidate map prefixes are undefined in Go. Every oracle
	// query uses an untruncated bound, then normalizes membership by ID.
	sort.Strings(ids)
	return ids
}

func ktableTemporalPut(
	id string,
	addr string,
	capture string,
	options ...ktableTemporalOption,
) ktableTemporalOperation {
	return ktableTemporalOperation{
		Kind: "putNode", ID: id, Addr: addr, Options: options, Capture: capture,
	}
}

func ktableTemporalSupport(value bool) ktableTemporalOption {
	return ktableTemporalOption{Kind: "support", Supported: &value}
}

func ktableTemporalResponded() ktableTemporalOption {
	return ktableTemporalOption{Kind: "responded"}
}

func ktableTemporalSample(discoveredNum int64, totalNum int64, nextDelaySeconds int64) ktableTemporalOption {
	return ktableTemporalOption{
		Kind: "sample", DiscoveredNum: discoveredNum, TotalNum: totalNum, NextDelaySeconds: nextDelaySeconds,
	}
}

func ktableTemporalID(value int) string {
	return ktableTemporalIDWithFirstByte(0, value)
}

func ktableTemporalIDWithFirstByte(first byte, value int) string {
	bytes := [20]byte{first}
	bytes[18] = byte(value >> 8)
	bytes[19] = byte(value)
	return protocol.ID(bytes).String()
}

func reconcileKTableTemporalFixtures(t *testing.T, fixtures []ktableTemporalFixture) {
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
	path := filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../testdata/parity/dht/ktable_temporal.jsonl"))
	if *updateDHTKTableTemporalParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-ktable-temporal-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("KTable temporal fixture is stale; rerun with -update-dht-ktable-temporal-parity")
	}
}
