package graphqlshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const (
	maxGraphQLResponseBytes = 16 << 20
	handlerDurationHeader   = "X-Bitmagnet-Graphql-Handler-Duration-Us"
)

// HTTPExecutor POSTs the original GraphQL request to the dark Rust service. Its
// lifetime is application-scoped; request cancellation/timeout is supplied by
// the Hook's detached bounded context.
type HTTPExecutor struct {
	endpoint string
	client   *http.Client
}

// NewHTTPExecutor constructs the production Rust client.
func NewHTTPExecutor(cfg Config) *HTTPExecutor {
	return &HTTPExecutor{
		endpoint: cfg.Endpoint,
		client:   http.DefaultClient,
	}
}

// Execute implements Executor.
func (e *HTTPExecutor) Execute(ctx context.Context, req Request) (ExecutionResult, error) {
	payload, err := json.Marshal(struct {
		Query         string          `json:"query"`
		OperationName string          `json:"operationName,omitempty"`
		Variables     json.RawMessage `json:"variables,omitempty"`
	}{
		Query:         req.Query,
		OperationName: req.OperationName,
		Variables:     req.Variables,
	})
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("encode GraphQL request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(payload))
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("build GraphQL request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/graphql-response+json, application/json")

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("POST dark GraphQL endpoint: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxGraphQLResponseBytes+1))
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("read dark GraphQL response: %w", err)
	}

	if len(body) > maxGraphQLResponseBytes {
		return ExecutionResult{}, fmt.Errorf("dark GraphQL response exceeds %d bytes", maxGraphQLResponseBytes)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return ExecutionResult{}, fmt.Errorf("dark GraphQL endpoint returned HTTP %d", resp.StatusCode)
	}

	handlerDuration, err := parseHandlerDuration(resp.Header.Get(handlerDurationHeader))
	if err != nil {
		return ExecutionResult{}, err
	}

	var envelope struct {
		Errors []json.RawMessage `json:"errors"`
	}

	if err := json.Unmarshal(body, &envelope); err != nil {
		return ExecutionResult{}, fmt.Errorf("decode dark GraphQL envelope: %w", err)
	}

	if len(envelope.Errors) != 0 {
		return ExecutionResult{}, fmt.Errorf("dark GraphQL response contains %d error(s)", len(envelope.Errors))
	}

	result, err := SearchResultFromData(body)
	if err != nil {
		return ExecutionResult{}, err
	}

	return ExecutionResult{Result: result, HandlerDuration: handlerDuration}, nil
}

func parseHandlerDuration(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, fmt.Errorf("dark GraphQL response missing %s", handlerDurationHeader)
	}

	microseconds, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || microseconds <= 0 || microseconds > int64(^uint64(0)>>1)/int64(time.Microsecond) {
		return 0, fmt.Errorf("dark GraphQL response has invalid %s %q", handlerDurationHeader, raw)
	}

	return time.Duration(microseconds) * time.Microsecond, nil
}
