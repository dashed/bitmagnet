package parity

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/processor/batch"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
)

const queueBatchPlanSubsystem = "process_torrent_batch_plan"

type queueBatchPlanInput struct {
	Message batch.MessageParams `json:"message"`
	Pages   [][]protocol.ID     `json:"pages"`
}

type queueBatchPlanExpected struct {
	Queries     []batch.Selection          `json:"queries"`
	Jobs        []queueFingerprintExpected `json:"jobs"`
	MaxInfoHash protocol.ID                `json:"maxInfoHash"`
	ChunkSize   uint                       `json:"chunkSize"`
	Done        bool                       `json:"done"`
}

type queueBatchPlanScenario struct {
	id      string
	message batch.MessageParams
	pages   [][]protocol.ID
}

func queueBatchPlanScenarios() []queueBatchPlanScenario {
	updatedBefore := time.Date(2026, time.August, 12, 4, 5, 6, 123456789, time.UTC)
	return []queueBatchPlanScenario{
		{
			id: "nonzero_cursor_short_page",
			message: batch.MessageParams{
				InfoHashGreaterThan: fixedHash(0x10),
				UpdatedBefore:       updatedBefore,
				ChunkSize:           10,
				BatchSize:           3,
			},
			pages: [][]protocol.ID{{fixedHash(0x11), fixedHash(0x12)}},
		},
		{
			id: "full_page_then_empty",
			message: batch.MessageParams{
				UpdatedBefore: updatedBefore,
				ChunkSize:     10,
				BatchSize:     2,
			},
			pages: [][]protocol.ID{{fixedHash(1), fixedHash(2)}, {}},
		},
		{
			id: "chunk_overshoot_nullable_filters",
			message: batch.MessageParams{
				UpdatedBefore:      updatedBefore,
				ClassifyMode:       processor.ClassifyModeRematch,
				ClassifierWorkflow: "custom",
				ClassifierFlags: classifier.Flags{
					"apis_enabled":         false,
					"local_search_enabled": true,
				},
				ChunkSize: 3,
				BatchSize: 2,
				ContentTypes: []model.NullContentType{
					model.NewNullContentType(model.ContentTypeMovie),
					model.NewNullContentType(nil),
				},
				Orphans: true,
			},
			pages: [][]protocol.ID{
				{fixedHash(1), fixedHash(2)},
				{fixedHash(3), fixedHash(4)},
			},
		},
		{
			id: "exact_chunk_boundary_continues",
			message: batch.MessageParams{
				InfoHashGreaterThan: fixedHash(0x20),
				UpdatedBefore:       updatedBefore,
				ClassifierFlags: classifier.Flags{
					"apis_enabled": "false",
				},
				ChunkSize: 4,
				BatchSize: 2,
			},
			pages: [][]protocol.ID{
				{fixedHash(0x21), fixedHash(0x22)},
				{fixedHash(0x23), fixedHash(0x24)},
			},
		},
	}
}

func TestGenerateQueueBatchPlanFixtures(t *testing.T) {
	fixtures := make([]Fixture, 0, len(queueBatchPlanScenarios()))
	for _, scenario := range queueBatchPlanScenarios() {
		planner := batch.NewPlanner(scenario.message)
		queries := make([]batch.Selection, 0, len(scenario.pages))
		for _, page := range scenario.pages {
			if !planner.ShouldQuery() {
				t.Fatalf("scenario %q supplies a page after the planner stopped", scenario.id)
			}
			queries = append(queries, planner.Selection())
			if _, err := planner.AddPage(page); err != nil {
				t.Fatalf("scenario %q: add page: %v", scenario.id, err)
			}
		}
		plan, err := planner.Finalize()
		if err != nil {
			t.Fatalf("scenario %q: finalize: %v", scenario.id, err)
		}

		jobs := make([]queueFingerprintExpected, 0, len(plan.Jobs))
		for _, spec := range plan.Jobs {
			job, err := spec.QueueJob()
			if err != nil {
				t.Fatalf("scenario %q: materialize job: %v", scenario.id, err)
			}
			jobs = append(jobs, queueFingerprintExpected{
				Queue:              job.Queue,
				Payload:            job.Payload,
				Fingerprint:        job.Fingerprint,
				MaxRetries:         job.MaxRetries,
				Priority:           job.Priority,
				ArchivalDurationNs: int64(job.ArchivalDuration),
			})
		}

		input, err := json.Marshal(queueBatchPlanInput{
			Message: scenario.message,
			Pages:   scenario.pages,
		})
		if err != nil {
			t.Fatalf("scenario %q: marshal input: %v", scenario.id, err)
		}
		expected, err := json.Marshal(queueBatchPlanExpected{
			Queries:     queries,
			Jobs:        jobs,
			MaxInfoHash: plan.MaxInfoHash,
			ChunkSize:   plan.ChunkSize,
			Done:        plan.Done,
		})
		if err != nil {
			t.Fatalf("scenario %q: marshal expected: %v", scenario.id, err)
		}
		fixtures = append(fixtures, Fixture{
			ID:        scenario.id,
			Subsystem: queueBatchPlanSubsystem,
			Input:     input,
			Expected:  expected,
		})
	}

	reconcileQueueFixtures(t, "process_torrent_batch_plans.jsonl", fixtures)
}
