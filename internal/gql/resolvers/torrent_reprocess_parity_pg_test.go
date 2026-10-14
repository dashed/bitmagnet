//go:build integration

package resolvers_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/classifier"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/database/search"
	"github.com/bitmagnet-io/bitmagnet/internal/gql"
	"github.com/bitmagnet-io/bitmagnet/internal/gql/resolvers"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/processor"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/tmdb"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	migrationssql "github.com/bitmagnet-io/bitmagnet/migrations"
)

const (
	reprocessParityDSNEnv    = "BITMAGNET_GRAPHQL_TORRENT_REPROCESS_TEST_ADMIN_DATABASE_URL"
	reprocessParityUpdateEnv = "BITMAGNET_GRAPHQL_TORRENT_REPROCESS_PARITY_UPDATE"
	reprocessParityDatabase  = "bitmagnet_graphql_torrent_reprocess_test"
)

type reprocessParityCase struct {
	ID        string          `json:"id"`
	Query     string          `json:"query"`
	Variables json.RawMessage `json:"variables,omitempty"`
	// Repeat re-sends the same request, for example to hit the retry insert's
	// active-fingerprint conflict.
	Repeat int `json:"repeat,omitempty"`
	// RustRefuses marks inputs whose Go result depends on content search or
	// TMDB, which the Rust runtime refuses before any write. Go is not run for
	// them: its outcome would only reflect this harness's unreachable stubs.
	RustRefuses bool `json:"rustRefuses,omitempty"`
}

// reprocessParityResponse keeps each error object whole, so the oracle pins
// gqlgen's exact error shape (message, path, and any locations/extensions).
type reprocessParityResponse struct {
	Data   json.RawMessage   `json:"data"`
	Errors []json.RawMessage `json:"errors"`
}

type reprocessParityOutcome struct {
	ID        string                    `json:"id"`
	Responses []reprocessParityResponse `json:"responses"`
	State     json.RawMessage           `json:"state"`
}

// TestTorrentReprocessParityGo runs every corpus case through the production
// gqlgen schema and the real Go processor against a disposable PostgreSQL
// database, then compares the response and the normalized database image with
// the committed oracle. The Rust disposable-PG test replays the same seed and
// requests and must reproduce the same oracle.
//
// Set BITMAGNET_GRAPHQL_TORRENT_REPROCESS_PARITY_UPDATE=1 to rewrite it.
func TestTorrentReprocessParityGo(t *testing.T) {
	dsn := os.Getenv(reprocessParityDSNEnv)
	if dsn == "" {
		t.Skip("torrent reprocess parity admin DSN is not set")
	}
	// Only the embedded core classifier: no XDG or working-directory overlay.
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	db, err := gormDB.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	var databaseName string
	require.NoError(t, db.QueryRow("SELECT current_database()").Scan(&databaseName))
	require.Equal(t, reprocessParityDatabase, databaseName,
		"reprocess parity oracle refuses to reset a database without its exact disposable-name sentinel")

	_, err = db.Exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	require.NoError(t, err)
	goose.SetBaseFS(migrationssql.FS)
	require.NoError(t, goose.SetDialect("postgres"))
	goose.SetLogger(goose.NopLogger())
	require.NoError(t, goose.UpContext(context.Background(), db, "."))

	server := newReprocessParityServer(t, gormDB)
	dir := reprocessParityDir(t)
	seed := readReprocessParityFile(t, dir, "seed.sql")
	dump := readReprocessParityFile(t, dir, "dump.sql")

	outcomes := make([]reprocessParityOutcome, 0)
	for _, oracle := range loadReprocessParityCases(t, dir) {
		if oracle.RustRefuses {
			continue
		}
		_, err := db.Exec(seed)
		require.NoError(t, err, oracle.ID)

		outcome := reprocessParityOutcome{ID: oracle.ID}
		for range max(oracle.Repeat, 1) {
			outcome.Responses = append(outcome.Responses, postReprocessParityCase(t, server, oracle))
		}
		var state string
		require.NoError(t, db.QueryRow(dump).Scan(&state), oracle.ID)
		outcome.State = json.RawMessage(state)
		outcomes = append(outcomes, outcome)
	}

	expectedPath := filepath.Join(dir, "expected.jsonl")
	if os.Getenv(reprocessParityUpdateEnv) == "1" {
		var buffer bytes.Buffer
		for _, outcome := range outcomes {
			line, err := json.Marshal(outcome)
			require.NoError(t, err)
			buffer.Write(line)
			buffer.WriteByte('\n')
		}
		require.NoError(t, os.WriteFile(expectedPath, buffer.Bytes(), 0o644))
		t.Logf("wrote %d reprocess parity outcomes", len(outcomes))
		return
	}

	expected := loadReprocessParityOutcomes(t, expectedPath)
	require.Len(t, outcomes, len(expected))
	for index, outcome := range outcomes {
		actualLine, err := json.Marshal(outcome)
		require.NoError(t, err)
		expectedLine, err := json.Marshal(expected[index])
		require.NoError(t, err)
		require.JSONEq(t, string(expectedLine), string(actualLine), outcome.ID)
	}
}

func newReprocessParityServer(t *testing.T, gormDB *gorm.DB) http.Handler {
	t.Helper()
	query := dao.Use(gormDB)
	lazyQuery := lazy.New(func() (*dao.Query, error) { return query, nil })
	searchResult := search.New(search.Params{Query: lazyQuery})
	classifierConfig := classifier.NewDefaultConfig()
	classifierResult := classifier.New(classifier.Params{
		Config:     classifierConfig,
		TmdbConfig: tmdb.NewDefaultConfig(),
		Search:     searchResult.Search,
		TmdbClient: lazy.New(func() (tmdb.Client, error) { return unreachableTmdbClient{}, nil }),
		Lifecycle:  fxtest.NewLifecycle(t),
		Logger:     zap.NewNop().Sugar(),
	})
	processorResult := processor.New(processor.Params{
		ClassifierConfig: classifierConfig,
		Search:           searchResult.Search,
		Workflow:         classifierResult.Runner,
		Dao:              lazyQuery,
		BlockingManager: lazy.New(func() (blocking.Manager, error) {
			return unreachableBlockingManager{}, nil
		}),
		Logger: zap.NewNop().Sugar(),
	})
	pr, err := processorResult.Processor.Get()
	require.NoError(t, err)

	server := handler.New(gql.NewExecutableSchema(gql.Config{
		Resolvers: &resolvers.Resolver{Processor: pr},
	}))
	server.AddTransport(transport.POST{})

	return server
}

func postReprocessParityCase(
	t *testing.T,
	server http.Handler,
	oracle reprocessParityCase,
) reprocessParityResponse {
	t.Helper()
	body := map[string]any{"query": oracle.Query}
	if len(oracle.Variables) > 0 {
		body["variables"] = oracle.Variables
	}
	encoded, err := json.Marshal(body)
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code, oracle.ID)

	var response reprocessParityResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), oracle.ID)
	if response.Errors == nil {
		response.Errors = []json.RawMessage{}
	}

	return response
}

func loadReprocessParityCases(t *testing.T, dir string) []reprocessParityCase {
	t.Helper()
	file, err := os.Open(filepath.Join(dir, "cases.jsonl"))
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()

	cases := make([]reprocessParityCase, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		var oracle reprocessParityCase
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &oracle))
		cases = append(cases, oracle)
	}
	require.NoError(t, scanner.Err())
	require.NotEmpty(t, cases)

	return cases
}

func loadReprocessParityOutcomes(t *testing.T, path string) []reprocessParityOutcome {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()

	outcomes := make([]reprocessParityOutcome, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for scanner.Scan() {
		var outcome reprocessParityOutcome
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &outcome))
		outcomes = append(outcomes, outcome)
	}
	require.NoError(t, scanner.Err())

	return outcomes
}

func readReprocessParityFile(t *testing.T, dir, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)

	return string(raw)
}

func reprocessParityDir(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Join(
		filepath.Dir(filename), "..", "..", "..", "testdata", "parity", "graphql-torrent-reprocess",
	)
}

var errReprocessParityUnreachable = errors.New("unreachable in the attachment-free reprocess corpus")

// unreachableTmdbClient fails every call: the corpus keeps apis_enabled off.
type unreachableTmdbClient struct{}

func (unreachableTmdbClient) ValidateAPIKey(context.Context) error {
	return errReprocessParityUnreachable
}

func (unreachableTmdbClient) SearchMovie(
	context.Context, tmdb.SearchMovieRequest,
) (tmdb.SearchMovieResponse, error) {
	return tmdb.SearchMovieResponse{}, errReprocessParityUnreachable
}

func (unreachableTmdbClient) MovieDetails(
	context.Context, tmdb.MovieDetailsRequest,
) (tmdb.MovieDetailsResponse, error) {
	return tmdb.MovieDetailsResponse{}, errReprocessParityUnreachable
}

func (unreachableTmdbClient) SearchTv(
	context.Context, tmdb.SearchTvRequest,
) (tmdb.SearchTvResponse, error) {
	return tmdb.SearchTvResponse{}, errReprocessParityUnreachable
}

func (unreachableTmdbClient) TvDetails(
	context.Context, tmdb.TvDetailsRequest,
) (tmdb.TvDetailsResponse, error) {
	return tmdb.TvDetailsResponse{}, errReprocessParityUnreachable
}

func (unreachableTmdbClient) FindByID(
	context.Context, tmdb.FindByIDRequest,
) (tmdb.FindByIDResponse, error) {
	return tmdb.FindByIDResponse{}, errReprocessParityUnreachable
}

// unreachableBlockingManager fails every call: no corpus flag deletes torrents.
type unreachableBlockingManager struct{}

func (unreachableBlockingManager) Filter(
	context.Context, []protocol.ID,
) ([]protocol.ID, error) {
	return nil, errReprocessParityUnreachable
}

func (unreachableBlockingManager) Block(context.Context, []protocol.ID, bool) error {
	return errReprocessParityUnreachable
}

func (unreachableBlockingManager) Flush(context.Context) error {
	return errReprocessParityUnreachable
}
