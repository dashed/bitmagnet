package graphqlshadow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPExecutorPostsOriginalRequestAndExtractsResult(t *testing.T) {
	t.Parallel()

	var received struct {
		Query         string         `json:"query"`
		OperationName string         `json:"operationName"`
		Variables     map[string]any `json:"variables"`
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}

		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", r.Header.Get("Content-Type"))
		}

		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode request: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(handlerDurationHeader, "7000")
		_, _ = w.Write([]byte(`{"data":{"torrentContent":{"search":{
          "totalCount":1,
		  "totalCountIsEstimate":false,
          "items":[{"id":"aa:movie:imdb:tt1"}],
          "aggregations":{}
        }}}}`))
	}))
	defer server.Close()

	executor := &HTTPExecutor{endpoint: server.URL, client: server.Client()}

	result, err := executor.Execute(context.Background(), Request{
		Query:         comparableQuery,
		OperationName: "Search",
		Variables:     json.RawMessage(`{"limit":20}`),
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if received.Query != comparableQuery || received.OperationName != "Search" {
		t.Errorf("received query/name = %q/%q", received.Query, received.OperationName)
	}

	if received.Variables["limit"] != float64(20) {
		t.Errorf("received variables = %#v", received.Variables)
	}

	if len(result.Result.IDs) != 1 || result.Result.IDs[0] != testInferID {
		t.Errorf("result IDs = %v", result.Result.IDs)
	}

	if result.HandlerDuration != 7*time.Millisecond {
		t.Errorf("handler duration = %s, want 7ms", result.HandlerDuration)
	}
}

func TestHTTPExecutorRejectsHTTPAndGraphQLErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "http", status: http.StatusBadGateway, body: `{}`},
		{name: "graphql", status: http.StatusOK, body: `{"errors":[{"message":"boom"}]}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set(handlerDurationHeader, "10")
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()

			executor := &HTTPExecutor{endpoint: server.URL, client: server.Client()}
			if _, err := executor.Execute(context.Background(), Request{Query: comparableQuery}); err == nil {
				t.Fatal("Execute returned nil error")
			}
		})
	}
}

func TestHTTPExecutorRequiresPositiveHandlerDuration(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"missing":   "",
		"malformed": "bogus",
		"negative":  "-1",
		"zero":      "0",
	}

	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")

				if value != "" {
					w.Header().Set(handlerDurationHeader, value)
				}

				_, _ = w.Write([]byte(`{"data":{}}`))
			}))
			defer server.Close()

			executor := &HTTPExecutor{endpoint: server.URL, client: server.Client()}
			if _, err := executor.Execute(context.Background(), Request{Query: comparableQuery}); err == nil {
				t.Fatal("Execute returned nil error")
			}
		})
	}
}

func TestNewHTTPExecutorDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()

	var followed atomic.Bool

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		followed.Store(true)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(handlerDurationHeader, "10")
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	defer target.Close()

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	cfg := NewDefaultConfig()
	cfg.Endpoint = redirect.URL

	if _, err := NewHTTPExecutor(cfg).Execute(
		context.Background(),
		Request{Query: comparableQuery},
	); err == nil {
		t.Fatal("Execute followed redirect or returned nil error")
	}

	if followed.Load() {
		t.Fatal("dark GraphQL client followed a redirect")
	}
}

func TestHTTPExecutorValidatesResponseContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		wantErr     bool
	}{
		{name: "graphql response JSON", contentType: "application/graphql-response+json; charset=utf-8"},
		{name: "missing", contentType: "", wantErr: true},
		{name: "plain text", contentType: "text/plain", wantErr: true},
		{name: "malformed", contentType: "not a media type", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if test.contentType != "" {
					w.Header().Set("Content-Type", test.contentType)
				}

				w.Header().Set(handlerDurationHeader, "10")
				_, _ = w.Write([]byte(`{"data":{"torrentContent":{"search":{` +
					`"totalCount":0,"totalCountIsEstimate":false,"items":[],"aggregations":{}}}}}`))
			}))
			defer server.Close()

			executor := &HTTPExecutor{endpoint: server.URL, client: server.Client()}

			_, err := executor.Execute(context.Background(), Request{Query: comparableQuery})
			if test.wantErr && err == nil {
				t.Fatal("Execute returned nil error")
			}

			if !test.wantErr && err != nil {
				t.Fatalf("Execute error: %v", err)
			}
		})
	}
}
