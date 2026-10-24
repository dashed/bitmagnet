package classifier

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	classifier_mocks "github.com/bitmagnet-io/bitmagnet/internal/classifier/mocks"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	tmdb_mocks "github.com/bitmagnet-io/bitmagnet/internal/tmdb/mocks"
	"github.com/stretchr/testify/mock"
)

// updateReplay regenerates the frozen real-name replay oracle
// (testdata/parity/classifier-replay/oracle.golden.jsonl) from names.jsonl /
// inputs.jsonl. It is a large (~120k names) production-sampled corpus used as the
// >=0.999 replay gate for the Rust classifier (Lane R/C). Unlike
// TestClassifierCorpus, inputs are consumed in file order (already deterministically
// sorted at freeze time) rather than re-sorted by id, and no golden is asserted
// against here: the goal is oracle generation, and determinism is verified by
// regenerating and diffing out-of-band.
var updateReplay = flag.Bool("update-replay", false, "regenerate the real-name replay oracle golden")

const classifierReplayDir = "../../testdata/parity/classifier-replay"

func TestClassifierReplayOracle(t *testing.T) {
	if !*updateReplay {
		t.Skip("pass -update-replay to (re)generate the replay oracle golden")
	}

	inputsPath := filepath.Join(classifierReplayDir, "inputs.jsonl")
	goldenPath := filepath.Join(classifierReplayDir, "oracle.golden.jsonl")
	inputs := loadReplayInputs(t, inputsPath)

	search := classifier_mocks.NewLocalSearch(t)
	tmdbClient := tmdb_mocks.NewClient(t)
	// The replay inputs carry no hint, so the hinted-content-id branch that reaches
	// attach_local_content_by_id never fires. Stub ContentByID with .Maybe() so it is
	// tolerated but not required; every other LocalSearch/tmdb method is left
	// expectation-free, so any flags-off impurity (e.g. a ContentBySearch call) fails
	// the strict mock — that is the purity assertion carried over from the base corpus.
	search.On("ContentByID", mock.Anything, mock.Anything).
		Return(model.Content{}, classification.ErrUnmatched).Maybe()

	corpusCompiler := compiler{
		options: []compilerOption{
			compilerFeatures(defaultFeatures),
			celEnvOption,
		},
		dependencies: dependencies{
			search:     search,
			tmdbClient: tmdbClient,
		},
	}

	source, err := yamlSourceProvider{rawSourceProvider: coreSourceProvider{}}.source()
	if err != nil {
		t.Fatalf("load core classifier source: %v", err)
	}

	workflow, err := corpusCompiler.Compile(source)
	if err != nil {
		t.Fatalf("compile core classifier source: %v", err)
	}

	flags := Flags{
		"local_search_enabled": false,
		"apis_enabled":         false,
		"tmdb_enabled":         false,
	}

	records := make([]classifierCorpusRecord, 0, len(inputs))
	for _, input := range inputs {
		result, runErr := workflow.Run(context.Background(), "default", flags, toTorrent(input))
		records = append(records, classifierCorpusRecord{
			ID:        input.ID,
			Subsystem: "classifier-replay",
			Input:     input,
			Expected:  normalizeClassifierResult(result, runErr),
		})
	}

	actual := encodeClassifierCorpus(t, records)
	if err := os.WriteFile(goldenPath, actual, 0o644); err != nil {
		t.Fatalf("write replay oracle golden: %v", err)
	}
	t.Logf("wrote %d replay oracle records to %s", len(records), goldenPath)
}

// loadReplayInputs reads the frozen replay inputs preserving file order (the freeze
// step already sorted them deterministically by name). It does not re-sort or
// dedupe: freezing guarantees unique ids and a stable order.
func loadReplayInputs(t *testing.T, path string) []classifierInput {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open replay inputs: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("close replay inputs: %v", err)
		}
	}()

	var inputs []classifierInput
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		if len(scanner.Bytes()) == 0 {
			t.Fatalf("replay input line %d is empty", line)
		}

		var input classifierInput
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			t.Fatalf("decode replay input line %d: %v", line, err)
		}
		if input.ID == "" {
			t.Fatalf("replay input line %d has an empty id", line)
		}
		inputs = append(inputs, input)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan replay inputs: %v", err)
	}
	if len(inputs) == 0 {
		t.Fatal("replay corpus has no inputs")
	}

	return inputs
}
