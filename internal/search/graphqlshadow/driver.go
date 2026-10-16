package graphqlshadow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Request is the original GraphQL request sent to the dark Rust service: the
// raw query document, selected operation name (which may be empty), and encoded
// variables. The driver never mutates it.
type Request struct {
	Query         string
	OperationName string
	Variables     json.RawMessage
}

// Executor runs a GraphQL request against the dark Rust service and returns the
// extracted search result plus the Rust handler's own measured duration. It is
// the candidate side of the comparison.
type Executor interface {
	Execute(ctx context.Context, req Request) (ExecutionResult, error)
}

// ExecutionResult is a dark Rust response and its server-side handler duration.
// The duration comes from X-Bitmagnet-Graphql-Handler-Duration-Us, never the Go
// client's HTTP round trip.
type ExecutionResult struct {
	Result          GraphQLResult
	HandlerDuration time.Duration
}

// Outcome is the disposition of one admitted shadow attempt.
type Outcome string

const (
	// OutcomeCompared means Rust executed and was compared with the captured Go
	// result.
	OutcomeCompared Outcome = "compared"
	// OutcomeDropped means the defensive operation/search gate rejected the
	// request before Rust execution.
	OutcomeDropped Outcome = "dropped"
	// OutcomeRustError means the dark Rust request failed.
	OutcomeRustError Outcome = "rust_error"
)

// Driver compares one dark Rust execution with the already-computed primary Go
// result. It never calls Go: the embedded gqlgen response hook supplies both the
// reference projection and the stored Go pre-write response-generation duration.
type Driver struct {
	rust    Executor
	metrics *Metrics
}

// NewDriver constructs a driver. metrics may be nil.
func NewDriver(rust Executor, metrics *Metrics) *Driver {
	return &Driver{
		rust:    rust,
		metrics: metrics,
	}
}

// ShadowCaptured executes Rust once and compares it with a captured Go result.
//
// The operation/search classification is deliberately repeated here as a final
// defense in depth. A mutation, subscription, parse failure, ambiguous document,
// unrelated query, or incomplete search projection returns OutcomeDropped after
// making ZERO calls to Rust. There is no Go reference client in this design, so
// a second Go execution is structurally impossible.
func (d *Driver) ShadowCaptured(
	ctx context.Context,
	req Request,
	reference GraphQLResult,
	referenceLatency time.Duration,
) (Outcome, *GraphQLComparison, error) {
	var variables map[string]any

	variablesValid := json.Unmarshal(req.Variables, &variables) == nil
	if !variablesValid || !IsComparableSearchOperation(req.Query, req.OperationName, variables) {
		d.metrics.incDropped()

		return OutcomeDropped, nil, nil
	}

	execution, err := d.rust.Execute(ctx, req)
	if err != nil {
		d.metrics.incRustError()

		return OutcomeRustError, nil, fmt.Errorf("graphqlshadow: rust execute: %w", err)
	}

	cmp := CompareGraphQL(
		execution.Result,
		reference,
		execution.HandlerDuration,
		referenceLatency,
	)
	d.metrics.observe(cmp)

	return OutcomeCompared, &cmp, nil
}
