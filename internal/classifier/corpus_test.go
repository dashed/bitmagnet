package classifier

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier/classification"
	classifier_mocks "github.com/bitmagnet-io/bitmagnet/internal/classifier/mocks"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	tmdb_mocks "github.com/bitmagnet-io/bitmagnet/internal/tmdb/mocks"
	"github.com/stretchr/testify/mock"
)

var updateCorpus = flag.Bool("update-corpus", false, "regenerate the classifier parity golden")

const classifierCorpusDir = "../../testdata/parity/classifier"

type classifierInput struct {
	ID          string                `json:"id"`
	Name        string                `json:"name"`
	Size        uint                  `json:"size"`
	FilesStatus string                `json:"filesStatus"`
	Extension   *string               `json:"extension,omitempty"`
	FilesCount  *uint                 `json:"filesCount,omitempty"`
	Files       []classifierInputFile `json:"files,omitempty"`
	Hint        *classifierInputHint  `json:"hint,omitempty"`
}

type classifierInputFile struct {
	Index     uint   `json:"index"`
	Path      string `json:"path"`
	Extension string `json:"extension"`
	Size      uint   `json:"size"`
}

type classifierInputHint struct {
	ContentType   string `json:"contentType"`
	ContentSource string `json:"contentSource,omitempty"`
	ContentID     string `json:"contentId,omitempty"`
}

type classifierExpected = NormalizedResult

type classifierCorpusRecord struct {
	ID        string             `json:"id"`
	Subsystem string             `json:"subsystem"`
	Input     classifierInput    `json:"input"`
	Expected  classifierExpected `json:"expected"`
}

func TestClassifierCorpus(t *testing.T) {
	inputsPath := filepath.Join(classifierCorpusDir, "inputs.jsonl")
	goldenPath := filepath.Join(classifierCorpusDir, "corpus.golden.jsonl")
	inputs := loadClassifierInputs(t, inputsPath)

	search := classifier_mocks.NewLocalSearch(t)
	tmdbClient := tmdb_mocks.NewClient(t)
	// The hinted-content-id branch (classifier.core.yml) reaches
	// attach_local_content_by_id, which is NOT flag-gated: it calls
	// LocalSearch.ContentByID unconditionally once torrent.hasHintedContentId is
	// set for a movie/tv_show/xxx content type. Stub it to ErrUnmatched so those
	// fixtures exercise the branch deterministically and network-free without
	// attaching content. Every other dependency method is left expectation-free:
	// if the flags-off corpus ever triggers ContentBySearch or any tmdb call, the
	// mock fails the test — that is the purity assertion. This expectation is
	// required (no .Maybe): the hinted-content-id fixtures must actually reach the
	// branch, or AssertExpectations fails.
	search.On("ContentByID", mock.Anything, mock.Anything).
		Return(model.Content{}, classification.ErrUnmatched)
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
			Subsystem: "classifier",
			Input:     input,
			Expected:  normalizeClassifierResult(result, runErr),
		})
	}

	actual := encodeClassifierCorpus(t, records)
	if *updateCorpus {
		if err := os.WriteFile(goldenPath, actual, 0o644); err != nil {
			t.Fatalf("write classifier corpus golden: %v", err)
		}
		return
	}

	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read classifier corpus golden (run with -update-corpus to create it): %v", err)
	}
	if bytes.Equal(expected, actual) {
		return
	}

	line, want, got := firstClassifierCorpusDifference(expected, actual)
	t.Fatalf(
		"classifier corpus golden differs at line %d (run with -update-corpus to regenerate)\nwant: %s\n got: %s",
		line,
		want,
		got,
	)
}

func loadClassifierInputs(t *testing.T, path string) []classifierInput {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open classifier corpus inputs: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("close classifier corpus inputs: %v", err)
		}
	}()

	var inputs []classifierInput
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		if len(scanner.Bytes()) == 0 {
			t.Fatalf("classifier corpus input line %d is empty", line)
		}

		var input classifierInput
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			t.Fatalf("decode classifier corpus input line %d: %v", line, err)
		}
		if input.ID == "" {
			t.Fatalf("classifier corpus input line %d has an empty id", line)
		}
		inputs = append(inputs, input)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan classifier corpus inputs: %v", err)
	}
	if len(inputs) == 0 {
		t.Fatal("classifier corpus has no inputs")
	}

	sort.SliceStable(inputs, func(i, j int) bool {
		return inputs[i].ID < inputs[j].ID
	})
	for i := 1; i < len(inputs); i++ {
		if inputs[i-1].ID == inputs[i].ID {
			t.Fatalf("classifier corpus has duplicate id %q", inputs[i].ID)
		}
	}

	return inputs
}

func toTorrent(input classifierInput) model.Torrent {
	torrent := model.Torrent{
		Name:        input.Name,
		Size:        input.Size,
		FilesStatus: model.FilesStatus(input.FilesStatus),
	}
	if input.Extension != nil {
		torrent.Extension = model.NewNullString(*input.Extension)
	}
	if input.FilesCount != nil {
		torrent.FilesCount = model.NewNullUint(*input.FilesCount)
	}
	if len(input.Files) > 0 {
		torrent.Files = make([]model.TorrentFile, 0, len(input.Files))
		for _, inputFile := range input.Files {
			file := model.TorrentFile{
				InfoHash: torrent.InfoHash,
				Index:    inputFile.Index,
				Path:     inputFile.Path,
				Size:     inputFile.Size,
			}
			if inputFile.Extension != "" {
				file.Extension = model.NewNullString(inputFile.Extension)
			}
			torrent.Files = append(torrent.Files, file)
		}
	}
	if input.Hint != nil {
		torrent.Hint = model.TorrentHint{
			InfoHash:    torrent.InfoHash,
			ContentType: model.ContentType(input.Hint.ContentType),
		}
		if input.Hint.ContentSource != "" {
			torrent.Hint.ContentSource = model.NewNullString(input.Hint.ContentSource)
		}
		if input.Hint.ContentID != "" {
			torrent.Hint.ContentID = model.NewNullString(input.Hint.ContentID)
		}
	}

	return torrent
}

func normalizeClassifierResult(result classification.Result, err error) classifierExpected {
	return NormalizeResult(result, err)
}

func encodeClassifierCorpus(t *testing.T, records []classifierCorpusRecord) []byte {
	t.Helper()

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			t.Fatalf("encode classifier corpus record %q: %v", record.ID, err)
		}
	}

	return buf.Bytes()
}

func firstClassifierCorpusDifference(expected, actual []byte) (int, string, string) {
	expectedLines := strings.Split(strings.TrimSuffix(string(expected), "\n"), "\n")
	actualLines := strings.Split(strings.TrimSuffix(string(actual), "\n"), "\n")
	lineCount := len(expectedLines)
	if len(actualLines) < lineCount {
		lineCount = len(actualLines)
	}
	for i := 0; i < lineCount; i++ {
		if expectedLines[i] != actualLines[i] {
			return i + 1, expectedLines[i], actualLines[i]
		}
	}
	if len(expectedLines) != len(actualLines) {
		return lineCount + 1,
			fmt.Sprintf("<EOF after %d lines>", len(expectedLines)),
			fmt.Sprintf("<EOF after %d lines>", len(actualLines))
	}

	return lineCount + 1, "<different line ending>", "<different line ending>"
}
