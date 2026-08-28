package parity

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	blobqueue "github.com/bitmagnet-io/bitmagnet/internal/blobmigration/queue"
	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/processor/batch"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/queue"
)

// updateQueueParity regenerates the checked-in queue parity goldens instead of
// verifying against them. Mirrors the -update-corpus flag on the classifier
// corpus test.
var updateQueueParity = flag.Bool(
	"update-queue-parity",
	false,
	"regenerate the queue fingerprint/backoff parity goldens",
)

const (
	queueFingerprintSubsystem = "queue_fingerprint"
	queueBackoffSubsystem     = "queue_backoff"
)

// fixedHash returns a deterministic 20-byte info hash whose last byte is n, so
// fixtures reference stable, human-readable info hashes.
func fixedHash(n byte) protocol.ID {
	var id protocol.ID
	id[len(id)-1] = n
	return id
}

// queueFingerprintExpected freezes the byte-exact queue-job wire contract for a
// single constructed job: the payload JSON that is fingerprinted, the resulting
// fingerprint, and the non-payload DB columns the constructor sets. Any drift in
// encoding/json key order, casing, omitempty, map-key sorting, or default
// injection changes payload and therefore fingerprint, breaking DB dedup — the
// exact failure the Rust port (Lane Q) must avoid.
type queueFingerprintExpected struct {
	Queue              string `json:"queue"`
	Payload            string `json:"payload"`
	Fingerprint        string `json:"fingerprint"`
	MaxRetries         uint   `json:"maxRetries"`
	Priority           int    `json:"priority"`
	ArchivalDurationNs int64  `json:"archivalDurationNs"`
}

type queueFingerprintScenario struct {
	id string
	// input is the neutral, pre-default-injection description of the caller
	// arguments, recorded verbatim in the fixture so the Rust side can
	// reconstruct the same logical job.
	input any
	// build runs the real production constructor (which applies default
	// injection and sets MaxRetries), returning the job whose payload and
	// fingerprint are frozen.
	build func() (model.QueueJob, error)
}

func queueFingerprintScenarios() []queueFingerprintScenario {
	return []queueFingerprintScenario{
		{
			id: "process_torrent_single_hash",
			input: map[string]any{
				"jobType":    processor.MessageName,
				"infoHashes": []string{fixedHash(1).String()},
			},
			build: func() (model.QueueJob, error) {
				return processor.NewQueueJob(processor.MessageParams{
					InfoHashes: []protocol.ID{fixedHash(1)},
				})
			},
		},
		{
			id: "process_torrent_multi_hash_ordering",
			input: map[string]any{
				"jobType": processor.MessageName,
				// Deliberately unsorted: the payload preserves caller order, it
				// is not normalized. The fingerprint is order-sensitive.
				"infoHashes": []string{
					fixedHash(3).String(),
					fixedHash(1).String(),
					fixedHash(2).String(),
				},
			},
			build: func() (model.QueueJob, error) {
				return processor.NewQueueJob(processor.MessageParams{
					InfoHashes: []protocol.ID{fixedHash(3), fixedHash(1), fixedHash(2)},
				})
			},
		},
		{
			id: "process_torrent_rematch_workflow_flags",
			input: map[string]any{
				"jobType":            processor.MessageName,
				"classifyMode":       int(processor.ClassifyModeRematch),
				"classifierWorkflow": "custom",
				// Go marshals maps with keys sorted alphabetically; the fixture
				// input is written unsorted on purpose so the golden proves the
				// Rust port must sort map keys too.
				"classifierFlags": map[string]any{
					"tmdb_enabled":         false,
					"apis_enabled":         false,
					"local_search_enabled": true,
				},
				"infoHashes": []string{fixedHash(1).String()},
			},
			build: func() (model.QueueJob, error) {
				return processor.NewQueueJob(processor.MessageParams{
					ClassifyMode:       processor.ClassifyModeRematch,
					ClassifierWorkflow: "custom",
					ClassifierFlags: classifier.Flags{
						"tmdb_enabled":         false,
						"apis_enabled":         false,
						"local_search_enabled": true,
					},
					InfoHashes: []protocol.ID{fixedHash(1)},
				})
			},
		},
		{
			id: "process_torrent_priority_20",
			input: map[string]any{
				"jobType":    processor.MessageName,
				"infoHashes": []string{fixedHash(1).String()},
				"priority":   20,
			},
			build: func() (model.QueueJob, error) {
				return processor.NewQueueJob(
					processor.MessageParams{InfoHashes: []protocol.ID{fixedHash(1)}},
					model.QueueJobPriority(20),
				)
			},
		},
		{
			id: "process_torrent_batch_defaults_injected",
			input: map[string]any{
				"jobType": batch.MessageName,
				"note":    "empty params; constructor injects BatchSize=100, ChunkSize=10000",
			},
			build: func() (model.QueueJob, error) {
				return batch.NewQueueJob(batch.MessageParams{})
			},
		},
		{
			id: "process_torrent_batch_keyset_continuation",
			input: map[string]any{
				"jobType":             batch.MessageName,
				"infoHashGreaterThan": fixedHash(0xab).String(),
				"classifyMode":        int(processor.ClassifyModeRematch),
				"chunkSize":           5000,
				"batchSize":           50,
				"contentTypes":        []string{"movie", "tv_show"},
			},
			build: func() (model.QueueJob, error) {
				return batch.NewQueueJob(batch.MessageParams{
					InfoHashGreaterThan: fixedHash(0xab),
					ClassifyMode:        processor.ClassifyModeRematch,
					ChunkSize:           5000,
					BatchSize:           50,
					ContentTypes: []model.NullContentType{
						model.NewNullContentType(model.ContentTypeMovie),
						model.NewNullContentType(model.ContentTypeTvShow),
					},
				})
			},
		},
		{
			id: "blob_migration_defaults_injected",
			input: map[string]any{
				"jobType": blobqueue.MessageName,
				"note":    "empty params; constructor injects ChunkSize=2000, NumRanges=1",
			},
			build: func() (model.QueueJob, error) {
				return blobqueue.NewQueueJob(blobqueue.MessageParams{})
			},
		},
		{
			id: "blob_migration_ranged",
			input: map[string]any{
				"jobType":             blobqueue.MessageName,
				"infoHashGreaterThan": fixedHash(0x10).String(),
				"infoHashLessOrEqual": fixedHash(0xf0).String(),
				"rangeId":             2,
				"numRanges":           4,
				"chunkSize":           1000,
			},
			build: func() (model.QueueJob, error) {
				return blobqueue.NewQueueJob(blobqueue.MessageParams{
					InfoHashGreaterThan: fixedHash(0x10).String(),
					InfoHashLessOrEqual: fixedHash(0xf0).String(),
					RangeID:             2,
					NumRanges:           4,
					ChunkSize:           1000,
				})
			},
		},
	}
}

func TestGenerateQueueFingerprintFixtures(t *testing.T) {
	scenarios := queueFingerprintScenarios()
	fixtures := make([]Fixture, 0, len(scenarios))

	for _, scenario := range scenarios {
		job, err := scenario.build()
		if err != nil {
			t.Fatalf("scenario %q: build job: %v", scenario.id, err)
		}

		// Self-check: the constructor's fingerprint must equal
		// hex(sha256(queue || payload)) — the frozen formula.
		want := fmt.Sprintf("%x", sha256.Sum256([]byte(job.Queue+job.Payload)))
		if job.Fingerprint != want {
			t.Fatalf(
				"scenario %q: fingerprint formula drift: constructor=%s recomputed=%s",
				scenario.id, job.Fingerprint, want,
			)
		}

		input, err := json.Marshal(scenario.input)
		if err != nil {
			t.Fatalf("scenario %q: marshal input: %v", scenario.id, err)
		}
		expected, err := json.Marshal(queueFingerprintExpected{
			Queue:              job.Queue,
			Payload:            job.Payload,
			Fingerprint:        job.Fingerprint,
			MaxRetries:         job.MaxRetries,
			Priority:           job.Priority,
			ArchivalDurationNs: int64(job.ArchivalDuration),
		})
		if err != nil {
			t.Fatalf("scenario %q: marshal expected: %v", scenario.id, err)
		}

		fixtures = append(fixtures, Fixture{
			ID:        scenario.id,
			Subsystem: queueFingerprintSubsystem,
			Input:     input,
			Expected:  expected,
		})
	}

	reconcileQueueFixtures(t, "fingerprints.jsonl", fixtures)
}

// queueBackoffExpected freezes the deterministic decomposition of
// queue.CalculateBackoff for a given retry count. The Sidekiq-style formula is
// now().Add((round(retries^4) + 15 + randInt(30)*retries + 1) seconds). Only the
// deterministic term is pinned; the randInt(30) jitter is explicitly bounded,
// never asserted, because it is nondeterministic (06 R4 / plan §1.1).
type queueBackoffExpected struct {
	DeterministicSeconds int `json:"deterministicSeconds"`
	JitterMinSeconds     int `json:"jitterMinSeconds"`
	JitterMaxSeconds     int `json:"jitterMaxSeconds"`
}

func TestGenerateQueueBackoffFixtures(t *testing.T) {
	fixtures := make([]Fixture, 0, 6)
	for retries := range 6 {
		deterministic := int(math.Round(math.Pow(float64(retries), 4))) + 15 + 1
		// RandInt(30) returns an int in [0,29]; the jitter term is
		// RandInt(30)*retries, so it lies in [0, 29*retries].
		jitterMax := 29 * retries

		input, err := json.Marshal(map[string]int{"retries": retries})
		if err != nil {
			t.Fatalf("retries %d: marshal input: %v", retries, err)
		}
		expected, err := json.Marshal(queueBackoffExpected{
			DeterministicSeconds: deterministic,
			JitterMinSeconds:     0,
			JitterMaxSeconds:     jitterMax,
		})
		if err != nil {
			t.Fatalf("retries %d: marshal expected: %v", retries, err)
		}
		fixtures = append(fixtures, Fixture{
			ID:        fmt.Sprintf("backoff_retries_%d", retries),
			Subsystem: queueBackoffSubsystem,
			Input:     input,
			Expected:  expected,
		})
	}

	reconcileQueueFixtures(t, "backoff.jsonl", fixtures)
}

// TestQueueBackoffJitterWithinBounds proves the real CalculateBackoff never
// escapes the frozen [deterministic, deterministic+jitterMax] envelope, without
// asserting the nondeterministic exact value.
func TestQueueBackoffJitterWithinBounds(t *testing.T) {
	for retries := range uint(6) {
		deterministic := int(math.Round(math.Pow(float64(retries), 4))) + 15 + 1
		jitterMax := 29 * int(retries)
		for range 200 {
			before := time.Now().UTC()
			got := queue.CalculateBackoff(retries)
			// Lower bound uses `before`; upper bound must use a timestamp taken
			// after the call so clock advance during the call cannot push the
			// observed delay above the envelope.
			after := time.Now().UTC()

			lowDelta := int(got.Sub(after).Round(time.Second) / time.Second)
			highDelta := int(got.Sub(before).Round(time.Second) / time.Second)
			if lowDelta < deterministic {
				t.Fatalf(
					"retries %d: backoff %ds below deterministic floor %ds",
					retries, lowDelta, deterministic,
				)
			}
			if highDelta > deterministic+jitterMax {
				t.Fatalf(
					"retries %d: backoff %ds above envelope ceiling %ds",
					retries, highDelta, deterministic+jitterMax,
				)
			}
		}
	}
}

// reconcileQueueFixtures verifies the freshly generated fixtures byte-match the
// checked-in golden, or rewrites the golden when -update-queue-parity is set.
func reconcileQueueFixtures(t *testing.T, filename string, fixtures []Fixture) {
	t.Helper()

	var buf bytes.Buffer
	for _, fixture := range fixtures {
		line, err := json.Marshal(fixture)
		if err != nil {
			t.Fatalf("marshal fixture %q: %v", fixture.ID, err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	actual := buf.Bytes()

	path := queueFixturePath(t, filename)
	if *updateQueueParity {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create queue fixture dir: %v", err)
		}
		if err := os.WriteFile(path, actual, 0o644); err != nil {
			t.Fatalf("write queue golden %s: %v", filename, err)
		}
		t.Logf("wrote %d fixtures to %s", len(fixtures), path)
		return
	}

	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read queue golden %s (run with -update-queue-parity to create it): %v", filename, err)
	}
	if bytes.Equal(expected, actual) {
		return
	}

	line, want, got := firstJSONLDifference(expected, actual)
	t.Fatalf(
		"queue golden %s differs at line %d (run with -update-queue-parity to regenerate)\nwant: %s\n got: %s",
		filename, line, want, got,
	)
}

func firstJSONLDifference(expected, actual []byte) (line int, want, got string) {
	expectedLines := strings.Split(strings.TrimSuffix(string(expected), "\n"), "\n")
	actualLines := strings.Split(strings.TrimSuffix(string(actual), "\n"), "\n")
	lineCount := len(expectedLines)
	if len(actualLines) < lineCount {
		lineCount = len(actualLines)
	}
	for i := range lineCount {
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

func queueFixturePath(t *testing.T, filename string) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve queue generator source path")
	}
	return filepath.Clean(filepath.Join(
		filepath.Dir(source), "..", "..", "testdata", "parity", "queue", filename,
	))
}
