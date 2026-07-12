package graphqlshadow

import (
	"errors"
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// OperationType is the kind of GraphQL operation a request would execute.
type OperationType string

const (
	// OperationQuery is a read-only query operation — the ONLY kind eligible for
	// shadowing (Rust execution + Go reference re-issue).
	OperationQuery OperationType = "query"
	// OperationMutation is a write operation. It must NEVER be re-issued to the Go
	// reference endpoint (it would double-apply a prod side effect), so it is
	// hard-dropped.
	OperationMutation OperationType = "mutation"
	// OperationSubscription is a subscription operation. Not served over POST
	// /graphql, but hard-dropped defensively regardless.
	OperationSubscription OperationType = "subscription"
)

// ErrNoOperation is returned when a document contains no executable operation.
var ErrNoOperation = errors.New("graphqlshadow: document contains no executable operation")

// ClassifyOperation parses a GraphQL request document and returns the type of
// the single operation that would execute for the given operationName, applying
// the GraphQL spec's operation-selection rules:
//
//   - operationName != "": the operation with that name is selected; if no
//     operation has that name it is an error.
//   - operationName == "": if the document defines exactly one operation it is
//     selected; if it defines none, or more than one, it is an error (the spec
//     requires operationName to disambiguate multiple operations).
//
// Any parse failure, or any of the above error conditions, returns a non-nil
// error. Callers enforcing the mutation-double-execute safety gate MUST fail
// closed on a non-nil error (treat it as ineligible / hard-drop) — see
// IsShadowEligible.
//
// The classification is deliberately conservative: it reflects which operation
// the reference Go /graphql endpoint would execute for the identical
// (query, operationName) pair, so a document that pairs a query with a mutation
// is only eligible when operationName selects the query (in which case the
// mutation never executes on either side).
func ClassifyOperation(query, operationName string) (OperationType, error) {
	doc, err := parser.ParseQuery(&ast.Source{Name: "shadow-request", Input: query})
	if err != nil {
		return "", fmt.Errorf("graphqlshadow: parse operation document: %w", err)
	}

	if len(doc.Operations) == 0 {
		return "", ErrNoOperation
	}

	var selected *ast.OperationDefinition

	if operationName == "" {
		if len(doc.Operations) > 1 {
			return "", fmt.Errorf(
				"graphqlshadow: document defines %d operations but no operationName was provided",
				len(doc.Operations))
		}

		selected = doc.Operations[0]
	} else {
		for _, op := range doc.Operations {
			if op.Name == operationName {
				selected = op

				break
			}
		}

		if selected == nil {
			return "", fmt.Errorf("graphqlshadow: no operation named %q in document", operationName)
		}
	}

	switch selected.Operation {
	case ast.Query:
		return OperationQuery, nil
	case ast.Mutation:
		return OperationMutation, nil
	case ast.Subscription:
		return OperationSubscription, nil
	default:
		// gqlparser only produces the three operations above; an empty/unknown
		// operation is treated as ineligible rather than assumed read-only.
		return "", fmt.Errorf("graphqlshadow: unknown operation type %q", selected.Operation)
	}
}

// IsShadowEligible reports whether a mirrored request may proceed to Rust
// execution and Go reference re-issue. It is the single decision point of the
// mutation-double-execute safety gate: it returns true ONLY for a read-only
// query operation. Every other outcome — a mutation, a subscription, a parse
// failure, a missing/ambiguous operation — returns false (fail closed), so no
// write operation can ever reach the Go reference endpoint.
func IsShadowEligible(query, operationName string) bool {
	op, err := ClassifyOperation(query, operationName)

	return err == nil && op == OperationQuery
}
