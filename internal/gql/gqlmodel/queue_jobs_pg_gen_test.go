//go:build integration

package gqlmodel

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	dbsearch "github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/gql/gqlmodel/gen"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

var updateQueueJobsParity = flag.Bool(
	"update-queue-jobs-parity",
	false,
	"regenerate the queue.jobs production-Go oracle corpus",
)

type queueJobFixture struct {
	ID          string  `json:"id"`
	Fingerprint string  `json:"fingerprint"`
	Queue       string  `json:"queue"`
	Status      string  `json:"status"`
	Payload     string  `json:"payload"`
	Priority    int     `json:"priority"`
	Retries     uint    `json:"retries"`
	MaxRetries  uint    `json:"maxRetries"`
	RunAfter    string  `json:"runAfter"`
	RanAt       *string `json:"ranAt"`
	Error       *string `json:"error"`
	CreatedAt   string  `json:"createdAt"`
}

type queueJobsOracleCase struct {
	ID       string                        `json:"id"`
	Input    json.RawMessage               `json:"input"`
	Expected queueJobsOracleExpectedResult `json:"expected"`
}

type queueJobsOracleInput struct {
	Queues      []string                      `json:"queues"`
	Statuses    []string                      `json:"statuses"`
	Limit       *uint                         `json:"limit"`
	Page        *uint                         `json:"page"`
	Offset      *uint                         `json:"offset"`
	TotalCount  *bool                         `json:"totalCount"`
	HasNextPage *bool                         `json:"hasNextPage"`
	Facets      *queueJobsOracleFacetsInput   `json:"facets"`
	OrderBy     []queueJobsOracleOrderByInput `json:"orderBy"`
}

type queueJobsOracleFacetsInput struct {
	Queue  *queueJobsOracleStringFacetInput `json:"queue"`
	Status *queueJobsOracleStatusFacetInput `json:"status"`
}

type queueJobsOracleStringFacetInput struct {
	Aggregate *bool    `json:"aggregate"`
	Filter    []string `json:"filter"`
}

type queueJobsOracleStatusFacetInput struct {
	Aggregate *bool    `json:"aggregate"`
	Filter    []string `json:"filter"`
}

type queueJobsOracleOrderByInput struct {
	Field      gen.QueueJobsOrderByField `json:"field"`
	Descending *bool                     `json:"descending"`
}

type queueJobsOracleExpectedResult struct {
	TotalCount   uint                          `json:"totalCount"`
	HasNextPage  bool                          `json:"hasNextPage"`
	Items        []queueJobsOracleExpectedItem `json:"items"`
	Aggregations queueJobsOracleExpectedAggs   `json:"aggregations"`
}

type queueJobsOracleExpectedItem struct {
	ID         string  `json:"id"`
	Queue      string  `json:"queue"`
	Status     string  `json:"status"`
	Payload    string  `json:"payload"`
	Priority   int     `json:"priority"`
	Retries    uint    `json:"retries"`
	MaxRetries uint    `json:"maxRetries"`
	RunAfter   string  `json:"runAfter"`
	RanAt      *string `json:"ranAt"`
	Error      *string `json:"error"`
	CreatedAt  string  `json:"createdAt"`
}

type queueJobsOracleExpectedAggs struct {
	Queue  []queueJobsOracleExpectedStringAgg `json:"queue"`
	Status []queueJobsOracleExpectedStatusAgg `json:"status"`
}

type queueJobsOracleExpectedStringAgg struct {
	Value string `json:"value"`
	Label string `json:"label"`
	Count int    `json:"count"`
}

type queueJobsOracleExpectedStatusAgg struct {
	Value string `json:"value"`
	Label string `json:"label"`
	Count int    `json:"count"`
}

func seedQueueJobsAndVerifyGoOracle(t *testing.T, gormDB *gorm.DB, db *sql.DB) {
	t.Helper()
	fixtures := loadQueueJobFixtures(t)
	for _, fixture := range fixtures {
		status, err := model.ParseQueueJobStatus(fixture.Status)
		require.NoError(t, err)
		runAfter := parseQueueJobsTime(t, fixture.RunAfter)
		createdAt := parseQueueJobsTime(t, fixture.CreatedAt)
		var ranAt any
		if fixture.RanAt != nil {
			ranAt = parseQueueJobsTime(t, *fixture.RanAt)
		}
		_, err = db.Exec(
			`INSERT INTO queue_jobs
			 (id, fingerprint, queue, status, payload, retries, max_retries,
			  run_after, ran_at, error, deadline, archival_duration, created_at, priority)
			 VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, NULL,
			         interval '1 hour', $11, $12)`,
			fixture.ID, fixture.Fingerprint, fixture.Queue, status.String(), fixture.Payload,
			fixture.Retries, fixture.MaxRetries, runAfter, ranAt, fixture.Error, createdAt,
			fixture.Priority,
		)
		require.NoError(t, err)
	}

	searchResult := dbsearch.New(dbsearch.Params{
		Query: lazy.New(func() (*dao.Query, error) { return dao.Use(gormDB), nil }),
	})
	searcher, err := searchResult.Search.Get()
	require.NoError(t, err)
	query := QueueQuery{QueueJobSearch: searcher}

	cases := queueJobsOracleCases()
	for i := range cases {
		actual, queryErr := query.Jobs(
			context.Background(),
			decodeQueueJobsOracleInput(t, cases[i].Input),
		)
		require.NoError(t, queryErr, "oracle case %q", cases[i].ID)
		cases[i].Expected = queueJobsOracleExpectedFromGo(actual)
	}
	reconcileQueueJobsCorpus(t, cases)
}

func queueJobsOracleCases() []queueJobsOracleCase {
	return []queueJobsOracleCase{
		{ID: "default-omitted", Input: json.RawMessage(`{}`)},
		{ID: "explicit-nulls", Input: json.RawMessage(`{"queues":null,"statuses":null,"limit":null,"page":null,"offset":null,"totalCount":null,"hasNextPage":null,"facets":null,"orderBy":null}`)},
		{ID: "page-plus-offset", Input: json.RawMessage(`{"limit":2,"page":2,"offset":1,"totalCount":true,"hasNextPage":true,"orderBy":[{"field":"created_at"}]}`)},
		{ID: "zero-limit-probes-next", Input: json.RawMessage(`{"limit":0,"page":2,"offset":0,"totalCount":true,"hasNextPage":true,"orderBy":[{"field":"created_at"}]}`)},
		{ID: "top-level-filters", Input: json.RawMessage(`{"queues":["process_torrent_batch"],"statuses":["pending","retry"],"limit":100,"totalCount":true,"hasNextPage":true,"orderBy":[{"field":"created_at","descending":true}]}`)},
		{ID: "facets-ignore-own-filter", Input: json.RawMessage(`{"statuses":["pending","retry"],"limit":100,"totalCount":true,"facets":{"queue":{"aggregate":true,"filter":["process_torrent"]},"status":{"aggregate":true,"filter":["retry"]}},"orderBy":[{"field":"priority"},{"field":"created_at"}]}`)},
		{ID: "selected-zero-facet", Input: json.RawMessage(`{"statuses":["pending"],"limit":100,"facets":{"status":{"aggregate":true,"filter":["failed"]}},"orderBy":[{"field":"created_at"}]}`)},
		{ID: "duplicate-order-replaces-direction", Input: json.RawMessage(`{"limit":5,"hasNextPage":true,"orderBy":[{"field":"priority"},{"field":"created_at","descending":true},{"field":"priority","descending":true}]}`)},
		{ID: "ran-at-desc-nulls-first", Input: json.RawMessage(`{"limit":5,"orderBy":[{"field":"ran_at","descending":true},{"field":"created_at","descending":true}]}`)},
		{ID: "explicit-empty-filter", Input: json.RawMessage(`{"queues":[],"statuses":[],"limit":100,"totalCount":true,"hasNextPage":true,"facets":{"queue":{"aggregate":true,"filter":[]},"status":{"aggregate":true,"filter":[]}}}`)},
		{ID: "false-count-flags", Input: json.RawMessage(`{"limit":3,"totalCount":false,"hasNextPage":false,"orderBy":[{"field":"created_at"}]}`)},
	}
}

func decodeQueueJobsOracleInput(t *testing.T, raw json.RawMessage) QueueJobsQueryInput {
	t.Helper()
	var input queueJobsOracleInput
	require.NoError(t, json.Unmarshal(raw, &input))
	result := QueueJobsQueryInput{Queues: input.Queues}
	for _, rawStatus := range input.Statuses {
		status, err := model.ParseQueueJobStatus(rawStatus)
		require.NoError(t, err)
		result.Statuses = append(result.Statuses, status)
	}
	if input.Limit != nil {
		result.Limit = model.NewNullUint(*input.Limit)
	}
	if input.Page != nil {
		result.Page = model.NewNullUint(*input.Page)
	}
	if input.Offset != nil {
		result.Offset = model.NewNullUint(*input.Offset)
	}
	if input.TotalCount != nil {
		result.TotalCount = model.NewNullBool(*input.TotalCount)
	}
	if input.HasNextPage != nil {
		result.HasNextPage = model.NewNullBool(*input.HasNextPage)
	}
	if input.Facets != nil {
		result.Facets = &gen.QueueJobsFacetsInput{}
		if input.Facets.Queue != nil {
			facet := gen.QueueJobQueueFacetInput{}
			if input.Facets.Queue.Aggregate != nil {
				facet.Aggregate = graphql.OmittableOf(input.Facets.Queue.Aggregate)
			}
			if input.Facets.Queue.Filter != nil {
				facet.Filter = graphql.OmittableOf(input.Facets.Queue.Filter)
			}
			result.Facets.Queue = graphql.OmittableOf(&facet)
		}
		if input.Facets.Status != nil {
			facet := gen.QueueJobStatusFacetInput{}
			if input.Facets.Status.Aggregate != nil {
				facet.Aggregate = graphql.OmittableOf(input.Facets.Status.Aggregate)
			}
			if input.Facets.Status.Filter != nil {
				statuses := make([]model.QueueJobStatus, 0, len(input.Facets.Status.Filter))
				for _, rawStatus := range input.Facets.Status.Filter {
					status, err := model.ParseQueueJobStatus(rawStatus)
					require.NoError(t, err)
					statuses = append(statuses, status)
				}
				facet.Filter = graphql.OmittableOf(statuses)
			}
			result.Facets.Status = graphql.OmittableOf(&facet)
		}
	}
	for _, order := range input.OrderBy {
		mapped := gen.QueueJobsOrderByInput{Field: order.Field}
		if order.Descending != nil {
			mapped.Descending = graphql.OmittableOf(order.Descending)
		}
		result.OrderBy = append(result.OrderBy, mapped)
	}
	return result
}

func queueJobsOracleExpectedFromGo(result QueueJobsQueryResult) queueJobsOracleExpectedResult {
	items := make([]queueJobsOracleExpectedItem, 0, len(result.Items))
	for _, item := range result.Items {
		var ranAt, itemError *string
		if item.RanAt.Valid {
			value := item.RanAt.Time.UTC().Format(time.RFC3339)
			ranAt = &value
		}
		if item.Error.Valid {
			value := item.Error.String
			itemError = &value
		}
		items = append(items, queueJobsOracleExpectedItem{
			ID: item.ID, Queue: item.Queue, Status: item.Status.String(), Payload: item.Payload,
			Priority: item.Priority, Retries: item.Retries, MaxRetries: item.MaxRetries,
			RunAfter: item.RunAfter.UTC().Format(time.RFC3339), RanAt: ranAt, Error: itemError,
			CreatedAt: item.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	var queueAggs []queueJobsOracleExpectedStringAgg
	if result.Aggregations.Queue != nil {
		queueAggs = make([]queueJobsOracleExpectedStringAgg, 0, len(result.Aggregations.Queue))
		for _, agg := range result.Aggregations.Queue {
			queueAggs = append(queueAggs, queueJobsOracleExpectedStringAgg(agg))
		}
	}
	var statusAggs []queueJobsOracleExpectedStatusAgg
	if result.Aggregations.Status != nil {
		statusAggs = make([]queueJobsOracleExpectedStatusAgg, 0, len(result.Aggregations.Status))
		for _, agg := range result.Aggregations.Status {
			statusAggs = append(statusAggs, queueJobsOracleExpectedStatusAgg{
				Value: agg.Value.String(), Label: agg.Label, Count: agg.Count,
			})
		}
	}
	return queueJobsOracleExpectedResult{
		TotalCount: result.TotalCount, HasNextPage: result.HasNextPage, Items: items,
		Aggregations: queueJobsOracleExpectedAggs{Queue: queueAggs, Status: statusAggs},
	}
}

func loadQueueJobFixtures(t *testing.T) []queueJobFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(queueJobsParityDir(t), "fixtures.json"))
	require.NoError(t, err)
	var fixtures []queueJobFixture
	require.NoError(t, json.Unmarshal(raw, &fixtures))
	require.NotEmpty(t, fixtures)
	return fixtures
}

func parseQueueJobsTime(t *testing.T, raw string) time.Time {
	t.Helper()
	value, err := time.Parse(time.RFC3339, raw)
	require.NoError(t, err)
	return value
}

func reconcileQueueJobsCorpus(t *testing.T, cases []queueJobsOracleCase) {
	t.Helper()
	var actual bytes.Buffer
	for _, oracle := range cases {
		require.NoError(t, json.NewEncoder(&actual).Encode(oracle))
	}
	path := filepath.Join(queueJobsParityDir(t), "corpus.jsonl")
	if *updateQueueJobsParity {
		require.NoError(t, os.WriteFile(path, actual.Bytes(), 0o644))
		return
	}
	expected, err := os.ReadFile(path)
	require.NoError(t, err, "run with -update-queue-jobs-parity to regenerate")
	if bytes.Equal(expected, actual.Bytes()) {
		return
	}
	wantScanner := bufio.NewScanner(bytes.NewReader(expected))
	gotScanner := bufio.NewScanner(bytes.NewReader(actual.Bytes()))
	for line := 1; wantScanner.Scan() || gotScanner.Scan(); line++ {
		if !bytes.Equal(wantScanner.Bytes(), gotScanner.Bytes()) {
			t.Fatalf("queue.jobs oracle differs at line %d\nwant: %s\n got: %s", line, wantScanner.Bytes(), gotScanner.Bytes())
		}
	}
	t.Fatal("queue.jobs oracle differs")
}

func queueJobsParityDir(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Join(filepath.Dir(filename), "..", "..", "..", "testdata", "parity", "graphql-queue-jobs")
}
