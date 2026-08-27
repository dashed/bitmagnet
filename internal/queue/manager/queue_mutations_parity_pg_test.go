//go:build integration

package manager

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
)

type queueMutationCase struct {
	ID                 string              `json:"id"`
	Operation          string              `json:"operation"`
	Input              *queueMutationInput `json:"input"`
	UpdatedBefore      string              `json:"updatedBefore"`
	PreexistingEnqueue bool                `json:"preexistingEnqueue"`
	ErrorContains      string              `json:"errorContains"`
	Expected           []queueMutationRow  `json:"expected"`
}

type queueMutationInput struct {
	Queues              []string               `json:"queues"`
	Statuses            []model.QueueJobStatus `json:"statuses"`
	Purge               bool                   `json:"purge"`
	BatchSize           uint                   `json:"batchSize"`
	ChunkSize           uint                   `json:"chunkSize"`
	ContentTypes        []*model.ContentType   `json:"contentTypes"`
	Orphans             bool                   `json:"orphans"`
	ClassifierRematch   bool                   `json:"classifierRematch"`
	ClassifierWorkflow  string                 `json:"classifierWorkflow"`
	ApisDisabled        bool                   `json:"apisDisabled"`
	LocalSearchDisabled bool                   `json:"localSearchDisabled"`
}

type queueMutationRow struct {
	Fingerprint string          `json:"fingerprint"`
	Queue       string          `json:"queue"`
	Status      string          `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	MaxRetries  uint            `json:"maxRetries"`
	Priority    int             `json:"priority"`
}

var queueMutationBaseline = []queueMutationRow{
	{
		Fingerprint: "1af3b5592f64dbd81b6318ab821e1dc563af4575fdce97ea5132be773eca07a9",
		Queue:       "alpha",
		Status:      "pending",
		Payload:     json.RawMessage(`{"case":"a"}`),
	},
	{
		Fingerprint: "f95b096cef0c64647322fe57954a4c8c8e0d37ce88d7e8ce2c545255a19f0545",
		Queue:       "alpha",
		Status:      "processed",
		Payload:     json.RawMessage(`{"case":"b"}`),
	},
	{
		Fingerprint: "67bbaa4cefba59fdc89e8e245ef26250e03b308e8ff56c0abb5cb732a05bc50c",
		Queue:       "beta",
		Status:      "retry",
		Payload:     json.RawMessage(`{"case":"c"}`),
	},
	{
		Fingerprint: "dfabeb2343e7d584d5b72149a03c6b428ee4f5fee1f2553591170187781e5b46",
		Queue:       "gamma",
		Status:      "failed",
		Payload:     json.RawMessage(`{"case":"d"}`),
	},
}

// TestQueueMutationParityCorpus proves the shared corpus against the Go
// production manager. The Rust disposable-PG test consumes the same cases.
func TestQueueMutationParityCorpus(t *testing.T) {
	dsn := os.Getenv("BITMAGNET_GRAPHQL_QUEUE_MUTATION_TEST_ADMIN_DATABASE_URL")
	if dsn == "" {
		t.Skip("queue mutation parity admin DSN is not set")
	}
	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	db, err := gormDB.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	var databaseName string
	require.NoError(t, db.QueryRow("SELECT current_database()").Scan(&databaseName))
	require.Equal(t, "bitmagnet_graphql_queue_mutation_test", databaseName,
		"queue mutation oracle refuses to reset a database without its exact disposable-name sentinel")

	_, err = db.Exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	require.NoError(t, err)
	goose.SetBaseFS(migrationssql.FS)
	require.NoError(t, goose.SetDialect("postgres"))
	goose.SetLogger(goose.NopLogger())
	require.NoError(t, goose.UpContext(context.Background(), db, "."))

	for _, oracle := range loadQueueMutationCases(t) {
		t.Run(oracle.ID, func(t *testing.T) {
			seedQueueMutationBaseline(t, gormDB)
			fixed := parseQueueMutationTime(t, oracle.UpdatedBefore)
			input := oracle.Input
			if input == nil {
				input = &queueMutationInput{}
			}
			if oracle.PreexistingEnqueue {
				job, jobErr := newReprocessTorrentsBatchJob(input.enqueueRequest(), fixed)
				require.NoError(t, jobErr)
				require.NoError(t, gormDB.Create(&job).Error)
			}

			manager := manager{
				dao: dao.Use(gormDB),
				db:  gormDB,
				now: func() time.Time { return fixed },
			}
			var mutationErr error
			switch oracle.Operation {
			case "purge":
				mutationErr = manager.PurgeJobs(context.Background(), PurgeJobsRequest{
					Queues:   input.Queues,
					Statuses: input.Statuses,
				})
			case "enqueue":
				mutationErr = manager.EnqueueReprocessTorrentsBatch(
					context.Background(), input.enqueueRequest(),
				)
			default:
				t.Fatalf("unknown operation %q", oracle.Operation)
			}
			if oracle.ErrorContains == "" {
				require.NoError(t, mutationErr)
			} else {
				require.ErrorContains(t, mutationErr, oracle.ErrorContains)
			}
			require.Equal(t, normalizeQueueMutationRows(t, oracle.Expected), readQueueMutationRows(t, db))
		})
	}

	t.Run("enqueue-purge-rolls-back-on-insert-failure", func(t *testing.T) {
		seedQueueMutationBaseline(t, gormDB)
		_, err := db.Exec(`ALTER TABLE queue_jobs ADD CONSTRAINT queue_mutation_reject_batch
			CHECK (queue <> 'process_torrent_batch') NOT VALID`)
		require.NoError(t, err)
		manager := manager{
			dao: dao.Use(gormDB),
			db:  gormDB,
			now: func() time.Time {
				return time.Date(2026, time.August, 27, 20, 15, 16, 0, time.UTC)
			},
		}
		err = manager.EnqueueReprocessTorrentsBatch(
			context.Background(),
			EnqueueReprocessTorrentsBatchRequest{Purge: true},
		)
		require.ErrorContains(t, err, "queue_mutation_reject_batch")
		_, dropErr := db.Exec("ALTER TABLE queue_jobs DROP CONSTRAINT queue_mutation_reject_batch")
		require.NoError(t, dropErr)
		require.Equal(t, normalizeQueueMutationRows(t, queueMutationBaseline), readQueueMutationRows(t, db))
	})
}

func (input queueMutationInput) enqueueRequest() EnqueueReprocessTorrentsBatchRequest {
	contentTypes := make([]model.NullContentType, 0, len(input.ContentTypes))
	for _, contentType := range input.ContentTypes {
		if contentType == nil {
			contentTypes = append(contentTypes, model.NewNullContentType(nil))
		} else {
			contentTypes = append(contentTypes, model.NewNullContentType(*contentType))
		}
	}
	classifyMode := processor.ClassifyModeDefault
	if input.ClassifierRematch {
		classifyMode = processor.ClassifyModeRematch
	}
	return EnqueueReprocessTorrentsBatchRequest{
		Purge:               input.Purge,
		BatchSize:           input.BatchSize,
		ChunkSize:           input.ChunkSize,
		ContentTypes:        contentTypes,
		Orphans:             input.Orphans,
		ClassifyMode:        classifyMode,
		ClassifierWorkflow:  input.ClassifierWorkflow,
		ApisDisabled:        input.ApisDisabled,
		LocalSearchDisabled: input.LocalSearchDisabled,
	}
}

func seedQueueMutationBaseline(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec("TRUNCATE queue_jobs").Error)
	for _, row := range queueMutationBaseline {
		insertQueueMutationRow(t, db, row)
	}
}

func insertQueueMutationRow(t *testing.T, db *gorm.DB, row queueMutationRow) {
	t.Helper()
	createdAt := time.Date(2024, time.June, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, db.Exec(
		`INSERT INTO queue_jobs
		 (fingerprint, queue, status, payload, retries, max_retries, run_after,
		  ran_at, error, deadline, archival_duration, created_at, priority)
		 VALUES (?, ?, ?::queue_job_status, ?::jsonb, 0, ?, ?, NULL, NULL, NULL,
		         interval '7 days', ?, ?)`,
		row.Fingerprint,
		row.Queue,
		row.Status,
		string(row.Payload),
		row.MaxRetries,
		createdAt,
		createdAt,
		row.Priority,
	).Error)
}

func readQueueMutationRows(t *testing.T, db interface {
	Query(string, ...any) (*sql.Rows, error)
}) []queueMutationRow {
	t.Helper()
	rows, err := db.Query(
		`SELECT fingerprint, queue, status::text, payload::text, max_retries, priority
		 FROM queue_jobs ORDER BY queue, status::text, fingerprint`,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	result := make([]queueMutationRow, 0)
	for rows.Next() {
		var row queueMutationRow
		require.NoError(t, rows.Scan(
			&row.Fingerprint,
			&row.Queue,
			&row.Status,
			&row.Payload,
			&row.MaxRetries,
			&row.Priority,
		))
		result = append(result, row)
	}
	require.NoError(t, rows.Err())
	return normalizeQueueMutationRows(t, result)
}

func normalizeQueueMutationRows(t *testing.T, rows []queueMutationRow) []queueMutationRow {
	t.Helper()
	result := append([]queueMutationRow(nil), rows...)
	for i := range result {
		var payload any
		require.NoError(t, json.Unmarshal(result[i].Payload, &payload))
		canonical, err := json.Marshal(payload)
		require.NoError(t, err)
		result[i].Payload = canonical
	}
	sort.Slice(result, func(i, j int) bool {
		left := result[i].Queue + "\x00" + result[i].Status + "\x00" + result[i].Fingerprint
		right := result[j].Queue + "\x00" + result[j].Status + "\x00" + result[j].Fingerprint
		return left < right
	})
	return result
}

func parseQueueMutationTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	require.NoError(t, err)
	return parsed
}

func loadQueueMutationCases(t *testing.T) []queueMutationCase {
	t.Helper()
	file, err := os.Open(filepath.Join(queueMutationParityDir(t), "corpus.jsonl"))
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()

	cases := make([]queueMutationCase, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var oracle queueMutationCase
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &oracle))
		cases = append(cases, oracle)
	}
	require.NoError(t, scanner.Err())
	require.NotEmpty(t, cases)
	return cases
}

func queueMutationParityDir(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Join(
		filepath.Dir(filename), "..", "..", "..", "testdata", "parity", "graphql-queue-mutations",
	)
}
