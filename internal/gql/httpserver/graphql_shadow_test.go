package httpserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/bitmagnet-io/bitmagnet/internal/search/graphqlshadow"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

const transportSearchQuery = `query Search($count: Boolean) {
  torrentContent {
    search(input: {totalCount: $count}) {
      totalCount
      totalCountIsEstimate
      items { id }
      aggregations { releaseYear { value label count isEstimate } }
    }
  }
}`

var transportSearchData = json.RawMessage(`{"torrentContent":{"search":{
  "totalCount":1,
  "totalCountIsEstimate":false,
  "items":[{"id":"aa:movie:imdb:tt1"}],
  "aggregations":{"releaseYear":[{"value":2026,"label":"2026","count":1,"isEstimate":false}]}
}}}`)

type darkGraphQL struct {
	server   *httptest.Server
	requests chan struct{}
	calls    atomic.Int64
}

func newDarkGraphQL(t *testing.T, handler http.HandlerFunc) *darkGraphQL {
	t.Helper()

	dark := &darkGraphQL{requests: make(chan struct{}, 8)}
	dark.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dark.calls.Add(1)
		dark.requests <- struct{}{}

		handler(w, r)
	}))
	t.Cleanup(dark.server.Close)

	return dark
}

func successfulDarkGraphQL(t *testing.T) *darkGraphQL {
	t.Helper()

	return newDarkGraphQL(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Bitmagnet-Graphql-Handler-Duration-Us", "7000")
		_, _ = w.Write([]byte(`{"data":` + string(transportSearchData) + `}`))
	})
}

func testGraphQLHandler(
	t *testing.T,
	dark *darkGraphQL,
	timeout time.Duration,
) (http.Handler, *graphqlshadow.Metrics) {
	t.Helper()

	cfg := graphqlshadow.NewDefaultConfig()
	cfg.Enabled = true
	cfg.Endpoint = dark.server.URL
	cfg.SampleRate = 1
	cfg.Timeout = timeout
	cfg.MaxConcurrent = 2

	executor := graphqlshadow.NewHTTPExecutor(cfg)
	metrics := graphqlshadow.NewMetrics()
	hook := graphqlshadow.NewHook(cfg, executor, metrics, nil)

	return newHandler(testExecutableSchema(), hook), metrics
}

func testExecutableSchema() graphql.ExecutableSchema {
	schema := gqlparser.MustLoadSchema(&ast.Source{Input: `
      input SearchInput { totalCount: Boolean }
      type SearchItem { id: ID! }
      type ReleaseYearAgg { value: Int, label: String!, count: Int!, isEstimate: Boolean! }
      type Aggregations { releaseYear: [ReleaseYearAgg!] }
      type SearchResult {
        totalCount: Int!
        totalCountIsEstimate: Boolean!
        items: [SearchItem!]!
        aggregations: Aggregations!
      }
      type TorrentContentQuery { search(input: SearchInput!): SearchResult! }
      type QueueQuery { jobs: [ID!]! }
      type TorrentQuery { files: [ID!]! }
      type Query {
        torrentContent: TorrentContentQuery!
        queue: QueueQuery!
        torrent: TorrentQuery!
        version: String!
        resolverError: String
      }
      type Mutation { mutate: Boolean! }
    `})

	return &graphql.ExecutableSchemaMock{
		SchemaFunc: func() *ast.Schema { return schema },
		ComplexityFunc: func(string, string, int, map[string]any) (int, bool) {
			return 0, false
		},
		ExecFunc: func(ctx context.Context) graphql.ResponseHandler {
			opCtx := graphql.GetOperationContext(ctx)
			if opCtx.Operation.Name == "ResolverError" {
				return graphql.OneShot(&graphql.Response{
					Errors: gqlerror.List{{Message: "resolver boom"}},
				})
			}

			switch opCtx.Operation.Operation {
			case ast.Query:
				return graphql.OneShot(&graphql.Response{Data: transportSearchData})
			case ast.Mutation:
				return graphql.OneShot(&graphql.Response{Data: json.RawMessage(`{"mutate":true}`)})
			default:
				return graphql.OneShot(graphql.ErrorResponse(ctx, "unsupported operation"))
			}
		},
	}
}

func postGraphQL(t *testing.T, handler http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	return recorder
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}

	return string(encoded)
}

func requireDarkCall(t *testing.T, dark *darkGraphQL) {
	t.Helper()

	select {
	case <-dark.requests:
	case <-time.After(time.Second):
		t.Fatal("dark Rust request did not arrive")
	}
}

func requireNoDarkCall(t *testing.T, dark *darkGraphQL) {
	t.Helper()

	select {
	case <-dark.requests:
		t.Fatal("unsafe/ineligible operation reached dark Rust")
	case <-time.After(75 * time.Millisecond):
	}
}

func TestGraphQLShadowActualTransportComparableSearch(t *testing.T) {
	t.Parallel()

	dark := successfulDarkGraphQL(t)
	handler, _ := testGraphQLHandler(t, dark, time.Second)
	body := mustJSON(t, map[string]any{
		"query":         transportSearchQuery,
		"operationName": "Search",
		"variables":     map[string]any{"count": true},
	})

	response := postGraphQL(t, handler, body)
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"errors"`) {
		t.Fatalf("unexpected GraphQL response: status=%d body=%s", response.Code, response.Body.String())
	}

	requireDarkCall(t, dark)
}

func TestGraphQLShadowActualTransportMixedSearchMakesZeroRustCalls(t *testing.T) {
	t.Parallel()

	for _, sibling := range []string{"queue { jobs }", "torrent { files }"} {
		sibling := sibling
		t.Run(sibling, func(t *testing.T) {
			t.Parallel()

			dark := successfulDarkGraphQL(t)
			handler, _ := testGraphQLHandler(t, dark, time.Second)
			query := strings.Replace(transportSearchQuery, "\n}", "\n  "+sibling+"\n}", 1)
			body := mustJSON(t, map[string]any{
				"query":         query,
				"operationName": "Search",
				"variables":     map[string]any{"count": true},
			})

			response := postGraphQL(t, handler, body)
			if response.Code != http.StatusOK {
				t.Fatalf("unexpected GraphQL status: %d body=%s", response.Code, response.Body.String())
			}
			requireNoDarkCall(t, dark)
		})
	}
}

func TestGraphQLShadowActualTransportNamedMultiOperation(t *testing.T) {
	t.Parallel()

	dark := successfulDarkGraphQL(t)
	handler, _ := testGraphQLHandler(t, dark, time.Second)
	query := transportSearchQuery + ` mutation Write { mutate }`
	body := mustJSON(t, map[string]any{
		"query":         query,
		"operationName": "Search",
		"variables":     map[string]any{"count": true},
	})

	postGraphQL(t, handler, body)
	requireDarkCall(t, dark)
}

func TestGraphQLShadowActualTransportMutationMakesZeroRustCalls(t *testing.T) {
	t.Parallel()

	dark := successfulDarkGraphQL(t)
	handler, _ := testGraphQLHandler(t, dark, time.Second)
	postGraphQL(t, handler, `{"query":"mutation Write { mutate }","operationName":"Write"}`)
	requireNoDarkCall(t, dark)
}

func TestGraphQLShadowActualTransportAPQ(t *testing.T) {
	t.Parallel()

	dark := successfulDarkGraphQL(t)
	handler, _ := testGraphQLHandler(t, dark, time.Second)
	hash := sha256.Sum256([]byte(transportSearchQuery))
	hashString := hex.EncodeToString(hash[:])
	extensions := map[string]any{
		"persistedQuery": map[string]any{"version": 1, "sha256Hash": hashString},
	}

	seed := mustJSON(t, map[string]any{
		"query":         transportSearchQuery,
		"operationName": "Search",
		"variables":     map[string]any{"count": true},
		"extensions":    extensions,
	})

	postGraphQL(t, handler, seed)
	requireDarkCall(t, dark)

	hit := mustJSON(t, map[string]any{
		"operationName": "Search",
		"variables":     map[string]any{"count": true},
		"extensions":    extensions,
	})

	postGraphQL(t, handler, hit)
	requireDarkCall(t, dark)
}

func TestGraphQLShadowActualTransportResolverErrorMakesZeroRustCalls(t *testing.T) {
	t.Parallel()

	dark := successfulDarkGraphQL(t)
	handler, _ := testGraphQLHandler(t, dark, time.Second)
	query := strings.Replace(transportSearchQuery, "query Search", "query ResolverError", 1)
	body := mustJSON(t, map[string]any{
		"query":         query,
		"operationName": "ResolverError",
		"variables":     map[string]any{"count": true},
	})

	response := postGraphQL(t, handler, body)
	if !strings.Contains(response.Body.String(), "resolver boom") {
		t.Fatalf("expected resolver error, got %s", response.Body.String())
	}

	requireNoDarkCall(t, dark)
}

func TestGraphQLShadowActualTransportRustTimeout(t *testing.T) {
	t.Parallel()

	completed := make(chan struct{})
	dark := newDarkGraphQL(t, func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(200 * time.Millisecond):
		}
		close(completed)
	})
	handler, metrics := testGraphQLHandler(t, dark, 20*time.Millisecond)
	body := mustJSON(t, map[string]any{
		"query":         transportSearchQuery,
		"operationName": "Search",
		"variables":     map[string]any{"count": true},
	})

	postGraphQL(t, handler, body)
	requireDarkCall(t, dark)

	deadline := time.Now().Add(time.Second)
	for metricValue(t, metrics, "bitmagnet_graphql_shadow_rust_error_total") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("hard timeout did not produce a Rust error metric")
		}

		time.Sleep(5 * time.Millisecond)
	}

	<-completed
}

func metricValue(t *testing.T, metrics *graphqlshadow.Metrics, name string) float64 {
	t.Helper()

	registry := prometheus.NewPedanticRegistry()
	for _, collector := range metrics.Collectors() {
		if err := registry.Register(collector); err != nil {
			t.Fatalf("register metric collector: %v", err)
		}
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	for _, family := range families {
		if family.GetName() == name && len(family.GetMetric()) == 1 {
			return family.GetMetric()[0].GetCounter().GetValue()
		}
	}

	return 0
}
