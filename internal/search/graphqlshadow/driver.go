package graphqlshadow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Request is a mirrored GraphQL request as it arrives at the dark Rust service:
// the raw query document, the selected operation name (may be empty), and the
// raw variables JSON. The driver never mutates it.
type Request struct {
	Query         string
	OperationName string
	Variables     json.RawMessage
}

// Executor runs a GraphQL request against the Rust service's own resolvers (the
// dark self path) and returns the extracted result. It is the candidate side of
// the comparison.
type Executor interface {
	Execute(ctx context.Context, req Request) (GraphQLResult, error)
}

// ReferenceClient re-issues a GraphQL request to the live Go /graphql endpoint
// (the authoritative reference) and returns the extracted result.
//
// SAFETY: the driver guarantees this is only ever called for a read-only query
// operation. It must never receive a mutation — a mutation re-issued here would
// double-apply a production side effect.
type ReferenceClient interface {
	Query(ctx context.Context, req Request) (GraphQLResult, error)
}

// Outcome is the disposition of one shadow attempt.
type Outcome string

const (
	// OutcomeCompared: the request was an eligible query; both sides executed and
	// the comparison was produced.
	OutcomeCompared Outcome = "compared"
	// OutcomeDropped: the request was hard-dropped by the safety gate (not a
	// read-only query, or the operation could not be classified). Neither the Rust
	// executor nor the Go reference was called.
	OutcomeDropped Outcome = "dropped"
	// OutcomeRustError: the Rust executor returned an error; no reference call was
	// made and no comparison was produced.
	OutcomeRustError Outcome = "rust_error"
	// OutcomeReferenceError: the Go reference returned an error after a successful
	// Rust execution; no comparison was produced.
	OutcomeReferenceError Outcome = "reference_error"
)

// Driver runs a single shadow comparison per mirrored request, enforcing the
// mutation-double-execute safety gate. It holds no request state and is safe for
// concurrent use if its Executor/ReferenceClient are.
type Driver struct {
	rust      Executor
	reference ReferenceClient
	metrics   *Metrics
	now       func() time.Time
}

// NewDriver constructs a shadow driver. metrics may be nil (the driver is then a
// pure comparator with no metrics side effects).
func NewDriver(rust Executor, reference ReferenceClient, metrics *Metrics) *Driver {
	return &Driver{
		rust:      rust,
		reference: reference,
		metrics:   metrics,
		now:       time.Now,
	}
}

// ShadowOnce runs one shadow comparison for a mirrored request.
//
// The FIRST thing it does — before touching either the Rust executor or the Go
// reference — is classify the operation. Anything that is not a read-only query
// (a mutation, a subscription, or a document that fails to classify) is
// hard-dropped: the method returns (OutcomeDropped, nil, nil) having made ZERO
// calls to rust.Execute and ZERO calls to reference.Query. This is the
// load-bearing safety property of the whole shadow mechanism; the ordering here
// is deliberate and must not be reordered.
//
// For an eligible query the driver executes the Rust path, then (only on Rust
// success) re-issues the identical request to the Go reference, and returns the
// comparison. A Rust error skips the reference call entirely (no point paying the
// reference cost, and it keeps a Rust bug from doubling reference load).
func (d *Driver) ShadowOnce(ctx context.Context, req Request) (Outcome, *GraphQLComparison, error) {
	// SAFETY GATE — classify first, drop non-queries before any execution.
	if !IsShadowEligible(req.Query, req.OperationName) {
		d.metrics.incDropped()

		return OutcomeDropped, nil, nil
	}

	rustStart := d.now()

	rustResult, err := d.rust.Execute(ctx, req)
	if err != nil {
		d.metrics.incRustError()

		return OutcomeRustError, nil, fmt.Errorf("graphqlshadow: rust execute: %w", err)
	}

	rustLatency := d.now().Sub(rustStart)

	refStart := d.now()

	refResult, err := d.reference.Query(ctx, req)
	if err != nil {
		d.metrics.incReferenceError()

		return OutcomeReferenceError, nil, fmt.Errorf("graphqlshadow: go reference query: %w", err)
	}

	refLatency := d.now().Sub(refStart)

	cmp := CompareGraphQL(rustResult, refResult, rustLatency, refLatency)
	d.metrics.observe(cmp)

	return OutcomeCompared, &cmp, nil
}
