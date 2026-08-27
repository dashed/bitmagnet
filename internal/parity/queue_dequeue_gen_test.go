//go:build integration

package parity

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"sort"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const queueDequeueSubsystem = "queue_dequeue"

// queueDequeueSeed is one crafted queue_jobs row. runAfterOffsetSeconds is
// relative to the moment the seed is inserted: negative = already visible,
// positive = still in the future (must never be dequeued).
type queueDequeueSeed struct {
	ID                    string `json:"id"`
	Queue                 string `json:"queue"`
	Status                string `json:"status"`
	Priority              int    `json:"priority"`
	RunAfterOffsetSeconds int    `json:"runAfterOffsetSeconds"`
}

type queueDequeueInput struct {
	// Queue is the queue name whose dequeue order is asserted.
	Queue string             `json:"queue"`
	Seed  []queueDequeueSeed `json:"seed"`
}

type queueDequeueExpected struct {
	// Order is the exact sequence of job ids the Go dequeue SQL yields, draining
	// the queue one FOR UPDATE SKIP LOCKED / LIMIT 1 claim at a time.
	Order []string `json:"order"`
}

// queueDequeueScenario freezes the dequeue ordering contract:
//
//	WHERE queue = $1 AND status IN ('pending','retry') AND run_after <= now()
//	ORDER BY (status = 'retry'), priority, run_after
//	FOR UPDATE SKIP LOCKED
//	LIMIT 1
//
// The seed is crafted so every (status='retry', priority, run_after) sort key is
// distinct — there is no id tiebreak in the Go query, so a tie would make the
// order nondeterministic (see sqlx-parameterized-limit-plan-trap). All pending
// jobs sort before all retry jobs; within a group, priority ASC then run_after
// ASC. Future and wrong-queue and terminal-status rows must be excluded.
func queueDequeueScenario() (queueDequeueInput, queueDequeueExpected) {
	input := queueDequeueInput{
		Queue: "process_torrent",
		Seed: []queueDequeueSeed{
			{ID: "p-d", Queue: "process_torrent", Status: "pending", Priority: -10, RunAfterOffsetSeconds: -10},
			{ID: "p-a", Queue: "process_torrent", Status: "pending", Priority: 0, RunAfterOffsetSeconds: -60},
			{ID: "p-b", Queue: "process_torrent", Status: "pending", Priority: 0, RunAfterOffsetSeconds: -30},
			{ID: "p-c", Queue: "process_torrent", Status: "pending", Priority: 5, RunAfterOffsetSeconds: -120},
			{ID: "r-a", Queue: "process_torrent", Status: "retry", Priority: 0, RunAfterOffsetSeconds: -90},
			{ID: "r-b", Queue: "process_torrent", Status: "retry", Priority: 0, RunAfterOffsetSeconds: -45},
			{ID: "r-c", Queue: "process_torrent", Status: "retry", Priority: 3, RunAfterOffsetSeconds: -200},
			// Excluded: run_after in the future.
			{ID: "future", Queue: "process_torrent", Status: "pending", Priority: -100, RunAfterOffsetSeconds: 3600},
			// Excluded: terminal statuses are not in ('pending','retry').
			{ID: "done", Queue: "process_torrent", Status: "processed", Priority: -100, RunAfterOffsetSeconds: -300},
			{ID: "dead", Queue: "process_torrent", Status: "failed", Priority: -100, RunAfterOffsetSeconds: -300},
			// Excluded: different queue.
			{ID: "other", Queue: "process_torrent_batch", Status: "pending", Priority: -100, RunAfterOffsetSeconds: -300},
		},
	}
	expected := queueDequeueExpected{
		Order: []string{"p-d", "p-a", "p-b", "p-c", "r-a", "r-b", "r-c"},
	}
	return input, expected
}

func TestGenerateQueueDequeueOrderingFixture(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping queue dequeue fixture generation")
	}

	ctx := context.Background()
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open fixture PostgreSQL: %v", err)
	}

	input, expected := queueDequeueScenario()
	assertDistinctDequeueSortKeys(t, input)

	// Work only inside a private, throwaway schema — never touch `public`, which
	// on the shared local PG may hold other data the harness must not drop.
	const schema = "phase3_queue_parity"
	if err := db.WithContext(ctx).Exec(
		"DROP SCHEMA IF EXISTS " + schema + " CASCADE; CREATE SCHEMA " + schema + "; SET search_path TO " + schema,
	).Error; err != nil {
		t.Fatalf("reset parity schema: %v", err)
	}

	// The minimal queue_jobs shape after migrations 00012 + 00015 + 00019. The
	// ORDER BY / status filter is the contract under test; the indexes are
	// included for fidelity but are not required for correctness at this scale.
	ddl := []string{
		`CREATE TYPE queue_job_status AS ENUM ('pending','processed','retry','failed')`,
		`CREATE TABLE queue_jobs (
			id text NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
			fingerprint text NOT NULL,
			queue text NOT NULL,
			status queue_job_status NOT NULL DEFAULT 'pending',
			payload jsonb NOT NULL,
			retries integer NOT NULL DEFAULT 0,
			max_retries integer NOT NULL DEFAULT 0,
			run_after timestamp with time zone NOT NULL,
			ran_at timestamp with time zone,
			error text,
			deadline timestamp with time zone,
			archival_duration interval NOT NULL,
			created_at timestamp with time zone NOT NULL,
			priority integer NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX ON queue_jobs (queue, status)`,
		`CREATE INDEX ON queue_jobs (id, queue, status, priority, run_after)`,
		`CREATE UNIQUE INDEX queue_jobs_fingerprint_idx ON queue_jobs (fingerprint) WHERE status IN ('pending','retry')`,
	}
	for _, stmt := range ddl {
		if err := db.WithContext(ctx).Exec(stmt).Error; err != nil {
			t.Fatalf("apply queue DDL: %v\n%s", err, stmt)
		}
	}

	for _, seed := range input.Seed {
		if err := db.WithContext(ctx).Exec(
			`INSERT INTO queue_jobs
			 (id, fingerprint, queue, status, payload, run_after, archival_duration, created_at, priority)
			 VALUES (?, ?, ?, ?::queue_job_status, '{}'::jsonb, now() + make_interval(secs => ?), make_interval(secs => 604800), now(), ?)`,
			seed.ID, seed.ID, seed.Queue, seed.Status, seed.RunAfterOffsetSeconds, seed.Priority,
		).Error; err != nil {
			t.Fatalf("seed job %q: %v", seed.ID, err)
		}
	}

	got := drainQueueInGoOrder(t, ctx, db, input.Queue)
	if !equalStrings(got, expected.Order) {
		t.Fatalf("dequeue order mismatch:\nwant: %v\n got: %v", expected.Order, got)
	}

	writeQueueDequeueFixture(t, input, expected)
}

// drainQueueInGoOrder replays the production dequeue query — literal LIMIT 1 (not
// a bind parameter, to dodge the generic-plan tie nondeterminism) — claiming and
// completing one job at a time until the queue is empty, and returns the id
// sequence.
func drainQueueInGoOrder(t *testing.T, ctx context.Context, db *gorm.DB, queueName string) []string {
	t.Helper()

	order := make([]string, 0)
	for {
		var id string
		row := db.WithContext(ctx).Raw(
			`SELECT id FROM queue_jobs
			 WHERE queue = ? AND status IN ('pending','retry') AND run_after <= now()
			 ORDER BY (status = 'retry'), priority, run_after
			 FOR UPDATE SKIP LOCKED
			 LIMIT 1`,
			queueName,
		).Row()
		if scanErr := row.Scan(&id); scanErr != nil {
			// No more claimable rows.
			break
		}
		order = append(order, id)
		if err := db.WithContext(ctx).Exec(
			`UPDATE queue_jobs SET status = 'processed', ran_at = now() WHERE id = ?`, id,
		).Error; err != nil {
			t.Fatalf("mark job %q processed: %v", id, err)
		}
	}
	return order
}

// assertDistinctDequeueSortKeys fails if any two claimable seed rows in the
// target queue share a (status='retry', priority, run_after) sort key, which
// would make the golden nondeterministic (there is no id tiebreak).
func assertDistinctDequeueSortKeys(t *testing.T, input queueDequeueInput) {
	t.Helper()

	type key struct {
		isRetry  bool
		priority int
		offset   int
	}
	seen := make(map[key]string)
	for _, seed := range input.Seed {
		if seed.Queue != input.Queue {
			continue
		}
		if seed.Status != "pending" && seed.Status != "retry" {
			continue
		}
		if seed.RunAfterOffsetSeconds > 0 {
			continue
		}
		k := key{isRetry: seed.Status == "retry", priority: seed.Priority, offset: seed.RunAfterOffsetSeconds}
		if prev, ok := seen[k]; ok {
			t.Fatalf("seed rows %q and %q share dequeue sort key %+v", prev, seed.ID, k)
		}
		seen[k] = seed.ID
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func writeQueueDequeueFixture(t *testing.T, input queueDequeueInput, expected queueDequeueExpected) {
	t.Helper()

	// Keep the seed serialization stable across runs.
	sort.SliceStable(input.Seed, func(i, j int) bool { return input.Seed[i].ID < input.Seed[j].ID })

	inputJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal dequeue input: %v", err)
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		t.Fatalf("marshal dequeue expected: %v", err)
	}

	var buf bytes.Buffer
	line, err := json.Marshal(Fixture{
		ID:        "dequeue_ordering_pending_before_retry",
		Subsystem: queueDequeueSubsystem,
		Input:     inputJSON,
		Expected:  expectedJSON,
	})
	if err != nil {
		t.Fatalf("marshal dequeue fixture: %v", err)
	}
	buf.Write(line)
	buf.WriteByte('\n')

	path := queueFixturePath(t, "dequeue_ordering.jsonl")
	if err := os.MkdirAll(dirOf(path), 0o755); err != nil {
		t.Fatalf("create queue fixture dir: %v", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write dequeue golden: %v", err)
	}
	t.Logf("wrote queue dequeue fixture to %s", path)
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
