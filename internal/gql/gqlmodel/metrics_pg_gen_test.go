//go:build integration

package gqlmodel

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/bitmagnet-io/bitmagnet/internal/gql/gqlmodel/gen"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/metrics/queuemetrics"
	"github.com/bitmagnet-io/bitmagnet/internal/metrics/torrentmetrics"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var updateGraphQLMetricsParity = flag.Bool(
	"update-graphql-metrics-parity",
	false,
	"regenerate the queue.metrics and torrent.metrics production-Go oracle corpus",
)

const (
	graphqlMetricsOracleQueue      = "graphql_metrics_oracle"
	graphqlMetricsOracleOtherQueue = "graphql_metrics_other"
)

type graphqlMetricsFixtures struct {
	QueueJobs      []graphqlMetricsQueueJobFixture      `json:"queueJobs"`
	Sources        []graphqlMetricsSourceFixture        `json:"sources"`
	TorrentSources []graphqlMetricsTorrentSourceFixture `json:"torrentSources"`
}

type graphqlMetricsQueueJobFixture struct {
	ID          string  `json:"id"`
	Fingerprint string  `json:"fingerprint"`
	Queue       string  `json:"queue"`
	Status      string  `json:"status"`
	RunAfter    string  `json:"runAfter"`
	RanAt       *string `json:"ranAt"`
	CreatedAt   string  `json:"createdAt"`
}

type graphqlMetricsSourceFixture struct {
	Key  string `json:"key"`
	Name string `json:"name"`
}

type graphqlMetricsTorrentSourceFixture struct {
	InfoHash  string `json:"infoHash"`
	Source    string `json:"source"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
}

type graphqlMetricsOracleCase struct {
	ID       string          `json:"id"`
	Surface  string          `json:"surface"`
	Input    json.RawMessage `json:"input"`
	Expected any             `json:"expected"`
}

type graphqlMetricsQueueExpected struct {
	Buckets []graphqlMetricsQueueBucket `json:"buckets"`
}

type graphqlMetricsQueueBucket struct {
	Queue           string  `json:"queue"`
	Status          string  `json:"status"`
	CreatedAtBucket string  `json:"createdAtBucket"`
	RanAtBucket     *string `json:"ranAtBucket"`
	Count           uint    `json:"count"`
	Latency         *string `json:"latency"`
}

type graphqlMetricsTorrentExpected struct {
	Buckets []graphqlMetricsTorrentBucket `json:"buckets"`
}

type graphqlMetricsTorrentBucket struct {
	Source  string `json:"source"`
	Bucket  string `json:"bucket"`
	Updated bool   `json:"updated"`
	Count   uint   `json:"count"`
}

// TestGenerateGraphQLMetricsParityPg seeds only fixed oracle rows into an
// already-migrated disposable database. It intentionally does not recreate the
// public schema, so it can run after the Phase-2 Go search seeder. It does own
// and clear the disposable queue_jobs table: the legacy unparenthesized queue
// time predicate can leak unrelated non-pending rows through otherwise
// selective filters, making an additive queue fixture nondeterministic.
func TestGenerateGraphQLMetricsParityPg(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping disposable PostgreSQL metrics oracle")
	}

	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	db, err := gormDB.DB()
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec("SET TIME ZONE 'UTC'")
	require.NoError(t, err)

	fixtures := loadGraphQLMetricsFixtures(t)
	removeGraphQLMetricsFixtures(t, db, fixtures)
	clearDisposableQueueJobs(t, db)
	seedGraphQLMetricsFixtures(t, db, fixtures)

	dbLazy := lazy.New(func() (*gorm.DB, error) { return gormDB, nil })
	queueResult := queuemetrics.New(queuemetrics.Params{DB: dbLazy})
	queueClient, err := queueResult.Client.Get()
	require.NoError(t, err)
	torrentResult := torrentmetrics.New(torrentmetrics.Params{DB: dbLazy})
	torrentClient, err := torrentResult.Client.Get()
	require.NoError(t, err)

	cases := graphqlMetricsOracleCases()
	for i := range cases {
		switch cases[i].Surface {
		case "queue":
			var input gen.QueueMetricsQueryInput
			require.NoError(t, json.Unmarshal(cases[i].Input, &input), cases[i].ID)
			actual, requestErr := (QueueQuery{QueueMetricsClient: queueClient}).Metrics(
				context.Background(),
				input,
			)
			require.NoError(t, requestErr, "oracle case %q", cases[i].ID)
			cases[i].Expected = graphqlMetricsQueueExpectedFromGo(t, actual)
		case "torrent":
			var input gen.TorrentMetricsQueryInput
			require.NoError(t, json.Unmarshal(cases[i].Input, &input), cases[i].ID)
			actual, requestErr := (TorrentQuery{TorrentMetricsClient: torrentClient}).Metrics(
				context.Background(),
				input,
			)
			require.NoError(t, requestErr, "oracle case %q", cases[i].ID)
			cases[i].Expected = graphqlMetricsTorrentExpectedFromGo(t, actual)
		default:
			t.Fatalf("oracle case %q has unknown surface %q", cases[i].ID, cases[i].Surface)
		}
	}

	reconcileGraphQLMetricsCorpus(t, cases)
}

func graphqlMetricsOracleCases() []graphqlMetricsOracleCase {
	return []graphqlMetricsOracleCase{
		{
			ID:      "queue-enum-order-hour",
			Surface: "queue",
			Input:   json.RawMessage(`{"bucketDuration":"hour","queues":["graphql_metrics_oracle"]}`),
		},
		{
			ID:      "queue-omitted-optionals-minute",
			Surface: "queue",
			Input:   json.RawMessage(`{"bucketDuration":"minute","queues":["graphql_metrics_oracle"]}`),
		},
		{
			ID:      "queue-explicit-null-optionals-minute",
			Surface: "queue",
			Input:   json.RawMessage(`{"bucketDuration":"minute","queues":["graphql_metrics_oracle"],"statuses":null,"startTime":null,"endTime":null}`),
		},
		{
			ID:      "queue-empty-filters-day",
			Surface: "queue",
			Input:   json.RawMessage(`{"bucketDuration":"day","queues":[],"statuses":[]}`),
		},
		{
			ID:      "queue-filtered-positive-latency-day",
			Surface: "queue",
			Input:   json.RawMessage(`{"bucketDuration":"day","queues":["graphql_metrics_oracle"],"statuses":["processed"]}`),
		},
		{
			ID:      "queue-start-only-legacy-precedence",
			Surface: "queue",
			Input:   json.RawMessage(`{"bucketDuration":"hour","queues":["graphql_metrics_oracle"],"startTime":"2099-06-01T01:00:00Z"}`),
		},
		{
			ID:      "queue-window-and-filters-legacy-precedence",
			Surface: "queue",
			Input:   json.RawMessage(`{"bucketDuration":"hour","queues":["graphql_metrics_oracle"],"statuses":["failed"],"startTime":"2099-06-01T01:00:00Z","endTime":"2099-06-01T01:30:00Z"}`),
		},
		{
			ID:      "queue-omitted-queues-empty-statuses",
			Surface: "queue",
			Input:   json.RawMessage(`{"bucketDuration":"day","statuses":[]}`),
		},
		{
			ID:      "queue-null-queues-empty-statuses",
			Surface: "queue",
			Input:   json.RawMessage(`{"bucketDuration":"day","queues":null,"statuses":[]}`),
		},
		{
			ID:      "torrent-enum-order-hour",
			Surface: "torrent",
			Input:   json.RawMessage(`{"bucketDuration":"hour","sources":["graphql_metrics_a","graphql_metrics_b"]}`),
		},
		{
			ID:      "torrent-omitted-sources-minute",
			Surface: "torrent",
			Input:   json.RawMessage(`{"bucketDuration":"minute","startTime":"2099-06-01T00:00:00Z","endTime":"2099-06-02T00:00:00Z"}`),
		},
		{
			ID:      "torrent-null-sources-minute",
			Surface: "torrent",
			Input:   json.RawMessage(`{"bucketDuration":"minute","sources":null,"startTime":"2099-06-01T00:00:00Z","endTime":"2099-06-02T00:00:00Z"}`),
		},
		{
			ID:      "torrent-empty-sources-day",
			Surface: "torrent",
			Input:   json.RawMessage(`{"bucketDuration":"day","sources":[]}`),
		},
		{
			ID:      "torrent-inclusive-boundaries-minute",
			Surface: "torrent",
			Input:   json.RawMessage(`{"bucketDuration":"minute","sources":["graphql_metrics_a"],"startTime":"2099-06-01T01:00:00Z","endTime":"2099-06-01T01:00:00.000001Z"}`),
		},
		{
			ID:      "torrent-source-filter-day",
			Surface: "torrent",
			Input:   json.RawMessage(`{"bucketDuration":"day","sources":["graphql_metrics_b"]}`),
		},
	}
}

func graphqlMetricsQueueExpectedFromGo(
	t *testing.T,
	result *gen.QueueMetricsQueryResult,
) graphqlMetricsQueueExpected {
	t.Helper()
	buckets := make([]graphqlMetricsQueueBucket, 0, len(result.Buckets))
	for _, bucket := range result.Buckets {
		var ranAtBucket, latency *string
		if !bucket.RanAtBucket.IsZero() {
			value := marshalGraphQLMetricsTime(t, bucket.RanAtBucket)
			ranAtBucket = &value
		}
		if bucket.Latency != nil {
			value := marshalGraphQLMetricsDuration(t, *bucket.Latency)
			latency = &value
		}
		buckets = append(buckets, graphqlMetricsQueueBucket{
			Queue:           bucket.Queue,
			Status:          bucket.Status.String(),
			CreatedAtBucket: marshalGraphQLMetricsTime(t, bucket.CreatedAtBucket),
			RanAtBucket:     ranAtBucket,
			Count:           bucket.Count,
			Latency:         latency,
		})
	}
	return graphqlMetricsQueueExpected{Buckets: buckets}
}

func graphqlMetricsTorrentExpectedFromGo(
	t *testing.T,
	result *gen.TorrentMetricsQueryResult,
) graphqlMetricsTorrentExpected {
	t.Helper()
	buckets := make([]graphqlMetricsTorrentBucket, 0, len(result.Buckets))
	for _, bucket := range result.Buckets {
		buckets = append(buckets, graphqlMetricsTorrentBucket{
			Source:  bucket.Source,
			Bucket:  marshalGraphQLMetricsTime(t, bucket.Bucket),
			Updated: bucket.Updated,
			Count:   bucket.Count,
		})
	}
	return graphqlMetricsTorrentExpected{Buckets: buckets}
}

func marshalGraphQLMetricsTime(t *testing.T, value time.Time) string {
	t.Helper()
	var encoded bytes.Buffer
	// Match the established queue.jobs oracle convention. PostgreSQL instants
	// are normalized to UTC so regeneration is byte-stable across developer
	// machines whose local time zones differ from production and CI.
	graphql.MarshalTime(value.UTC()).MarshalGQL(&encoded)
	var decoded string
	require.NoError(t, json.Unmarshal(encoded.Bytes(), &decoded))
	return decoded
}

func marshalGraphQLMetricsDuration(t *testing.T, value time.Duration) string {
	t.Helper()
	var encoded bytes.Buffer
	graphql.MarshalDuration(value).MarshalGQL(&encoded)
	var decoded string
	require.NoError(t, json.Unmarshal(encoded.Bytes(), &decoded))
	return decoded
}

func loadGraphQLMetricsFixtures(t *testing.T) graphqlMetricsFixtures {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(graphqlMetricsParityDir(t), "fixtures.json"))
	require.NoError(t, err)
	var fixtures graphqlMetricsFixtures
	require.NoError(t, json.Unmarshal(raw, &fixtures))
	require.NotEmpty(t, fixtures.QueueJobs)
	require.NotEmpty(t, fixtures.Sources)
	require.NotEmpty(t, fixtures.TorrentSources)
	return fixtures
}

func removeGraphQLMetricsFixtures(t *testing.T, db *sql.DB, fixtures graphqlMetricsFixtures) {
	t.Helper()
	for _, fixture := range fixtures.QueueJobs {
		_, err := db.Exec("DELETE FROM queue_jobs WHERE id = $1", fixture.ID)
		require.NoError(t, err)
	}
	for _, fixture := range fixtures.TorrentSources {
		hash, err := protocol.ParseID(fixture.InfoHash)
		require.NoError(t, err)
		_, err = db.Exec("DELETE FROM torrents WHERE info_hash = $1", hash[:])
		require.NoError(t, err)
	}
	for _, fixture := range fixtures.Sources {
		_, err := db.Exec("DELETE FROM torrent_sources WHERE key = $1", fixture.Key)
		require.NoError(t, err)
	}
}

func clearDisposableQueueJobs(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("DELETE FROM queue_jobs")
	require.NoError(t, err)
}

func seedGraphQLMetricsFixtures(t *testing.T, db *sql.DB, fixtures graphqlMetricsFixtures) {
	t.Helper()
	for _, fixture := range fixtures.QueueJobs {
		runAfter := parseGraphQLMetricsTime(t, fixture.RunAfter)
		createdAt := parseGraphQLMetricsTime(t, fixture.CreatedAt)
		var ranAt any
		if fixture.RanAt != nil {
			ranAt = parseGraphQLMetricsTime(t, *fixture.RanAt)
		}
		_, err := db.Exec(
			`INSERT INTO queue_jobs
			 (id, fingerprint, queue, status, payload, retries, max_retries,
			  run_after, ran_at, error, deadline, archival_duration, created_at, priority)
			 VALUES ($1, $2, $3, $4, '{}'::jsonb, 0, 0, $5, $6, NULL, NULL,
			         interval '1 hour', $7, 0)`,
			fixture.ID,
			fixture.Fingerprint,
			fixture.Queue,
			fixture.Status,
			runAfter,
			ranAt,
			createdAt,
		)
		require.NoError(t, err)
	}

	now := parseGraphQLMetricsTime(t, "2099-06-01T00:00:00Z")
	for _, fixture := range fixtures.Sources {
		_, err := db.Exec(
			`INSERT INTO torrent_sources (key, name, created_at, updated_at)
			 VALUES ($1, $2, $3, $3)`,
			fixture.Key,
			fixture.Name,
			now,
		)
		require.NoError(t, err)
	}
	for _, fixture := range fixtures.TorrentSources {
		hash, err := protocol.ParseID(fixture.InfoHash)
		require.NoError(t, err)
		createdAt := parseGraphQLMetricsTime(t, fixture.CreatedAt)
		updatedAt := parseGraphQLMetricsTime(t, fixture.UpdatedAt)
		_, err = db.Exec(
			`INSERT INTO torrents
			 (info_hash, name, size, private, files_status, file_extensions,
			  created_at, updated_at, files_data)
			 VALUES ($1, $2, 1, false, 'no_info', '[]'::jsonb, $3, $4, NULL)`,
			hash[:],
			"GraphQL metrics oracle "+fixture.InfoHash,
			createdAt,
			updatedAt,
		)
		require.NoError(t, err)
		_, err = db.Exec(
			`INSERT INTO torrents_torrent_sources
			 (source, info_hash, created_at, updated_at)
			 VALUES ($1, $2, $3, $4)`,
			fixture.Source,
			hash[:],
			createdAt,
			updatedAt,
		)
		require.NoError(t, err)
	}
}

func parseGraphQLMetricsTime(t *testing.T, raw string) time.Time {
	t.Helper()
	value, err := time.Parse(time.RFC3339Nano, raw)
	require.NoError(t, err)
	return value
}

func reconcileGraphQLMetricsCorpus(t *testing.T, cases []graphqlMetricsOracleCase) {
	t.Helper()
	var actual bytes.Buffer
	for _, oracle := range cases {
		require.NoError(t, json.NewEncoder(&actual).Encode(oracle))
	}
	path := filepath.Join(graphqlMetricsParityDir(t), "corpus.jsonl")
	if *updateGraphQLMetricsParity {
		require.NoError(t, os.WriteFile(path, actual.Bytes(), 0o644))
		return
	}
	expected, err := os.ReadFile(path)
	require.NoError(t, err, "run with -update-graphql-metrics-parity to regenerate")
	if bytes.Equal(expected, actual.Bytes()) {
		return
	}
	wantScanner := bufio.NewScanner(bytes.NewReader(expected))
	gotScanner := bufio.NewScanner(bytes.NewReader(actual.Bytes()))
	for line := 1; wantScanner.Scan() || gotScanner.Scan(); line++ {
		if !bytes.Equal(wantScanner.Bytes(), gotScanner.Bytes()) {
			t.Fatalf(
				"GraphQL metrics oracle differs at line %d\nwant: %s\n got: %s",
				line,
				wantScanner.Bytes(),
				gotScanner.Bytes(),
			)
		}
	}
	t.Fatal("GraphQL metrics oracle differs")
}

func graphqlMetricsParityDir(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Join(
		filepath.Dir(filename),
		"..",
		"..",
		"..",
		"testdata",
		"parity",
		"graphql-metrics",
	)
}

func TestGraphQLMetricsOracleFixtureKeysAreDistinct(t *testing.T) {
	fixtures := loadGraphQLMetricsFixtures(t)
	seen := make(map[string]string)
	for _, fixture := range fixtures.QueueJobs {
		require.Contains(
			t,
			[]string{graphqlMetricsOracleQueue, graphqlMetricsOracleOtherQueue},
			fixture.Queue,
		)
		for kind, value := range map[string]string{
			"queue job ID":          fixture.ID,
			"queue job fingerprint": fixture.Fingerprint,
		} {
			if previous, exists := seen[value]; exists {
				t.Fatalf("duplicate %s %q (already used as %s)", kind, value, previous)
			}
			seen[value] = kind
		}
	}
	for _, fixture := range fixtures.Sources {
		if previous, exists := seen[fixture.Key]; exists {
			t.Fatalf("duplicate source key %q (already used as %s)", fixture.Key, previous)
		}
		seen[fixture.Key] = "source key"
	}
	for _, fixture := range fixtures.TorrentSources {
		if previous, exists := seen[fixture.InfoHash]; exists {
			t.Fatalf("duplicate info hash %q (already used as %s)", fixture.InfoHash, previous)
		}
		seen[fixture.InfoHash] = "info hash"
	}
	for _, oracle := range graphqlMetricsOracleCases() {
		if previous, exists := seen[oracle.ID]; exists {
			t.Fatalf("duplicate oracle ID %q (already used as %s)", oracle.ID, previous)
		}
		seen[oracle.ID] = fmt.Sprintf("%s oracle case", oracle.Surface)
	}
}
